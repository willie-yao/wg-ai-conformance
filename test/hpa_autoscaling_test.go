package conformance

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
)

const (
	hpaTestLabelKey           = "ai-conformance.kubernetes.io/test"
	hpaRunLabelKey            = "ai-conformance.kubernetes.io/run"
	hpaTestLabelValue         = "kar-0057"
	hpaResourceConsumerImage  = "registry.k8s.io/e2e-test-images/resource-consumer:1.14"
	hpaResourceConsumerPort   = int32(8080)
	hpaInitialReplicas        = int32(1)
	hpaMaximumReplicas        = int32(2)
	hpaMetricTargetValue      = int64(10)
	hpaMetricHighValue        = int64(100)
	hpaMetricBumpLifetime     = 7 * 24 * time.Hour
	hpaScaleDownStabilization = int32(60)
	hpaScaleTimeout           = 10 * time.Minute
	hpaStabilityWindow        = 30 * time.Second
	hpaPollInterval           = 3 * time.Second
	hpaPodProxyTimeout        = time.Minute
	hpaCapacityCheckTimeout   = time.Minute
	hpaCleanupTimeout         = 2 * time.Minute
)

var (
	hpaCustomMetricName *string
	hpaNamespace        *string
	hpaMetricTimeout    *time.Duration
	prometheusMetricRE  = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)
)

func init() {
	hpaCustomMetricName = flag.String("hpa-custom-metric-name", "",
		"Pod custom metric exposed by the platform's custom.metrics.k8s.io adapter. The KAR-0057 test is skipped when empty.")
	hpaNamespace = flag.String("hpa-namespace", "",
		"Existing namespace preconfigured for metric scraping. If empty, the test creates and deletes a namespace.")
	hpaMetricTimeout = flag.Duration("hpa-metric-timeout", 12*time.Minute,
		"Maximum time for a new or changed metric to become visible through custom.metrics.k8s.io.")
}

// TestAcceleratorHorizontalPodAutoscaling verifies KAR-0057.
// Ref: https://github.com/kubernetes-sigs/ai-conformance/issues/57
func TestAcceleratorHorizontalPodAutoscaling(t *testing.T) {
	if !flag.Parsed() {
		flag.Parse()
	}

	metricName := strings.TrimSpace(*hpaCustomMetricName)
	if metricName == "" {
		t.Skip("HPA custom-metrics test is not configured; set -hpa-custom-metric-name=<metric> to run it. A skipped test is not conformance evidence: use manual attestation when HPA is supported only through external, object, or push-based metrics, and mark pod_autoscaling N/A only when HPA itself is unsupported.")
	}
	if !prometheusMetricRE.MatchString(metricName) {
		t.Fatalf("Invalid -hpa-custom-metric-name %q: must be a Prometheus metric name", metricName)
	}
	if *hpaMetricTimeout <= 0 {
		t.Fatal("-hpa-metric-timeout must be positive")
	}

	runID := rand.String(5)
	objectLabels := map[string]string{hpaTestLabelKey: hpaTestLabelValue, hpaRunLabelKey: runID}
	selector := labels.SelectorFromSet(map[string]string{hpaRunLabelKey: runID}).String()
	workloadName := "kar-57-" + runID
	claimTemplateName := workloadName + "-claim-template"

	cfg, err := lookupAcceleratorConfig(*acceleratorType)
	if err != nil {
		t.Fatalf("Invalid -accelerator-type: %v", err)
	}
	clientset := getClientset(t)
	ctx := context.Background()
	mode, _, err := detectAllocationMode(ctx, clientset, *allocationMode, cfg, t.Logf)
	if err != nil {
		t.Fatalf("ENVIRONMENT ERROR: Failed to resolve allocation mode: %v", err)
	}

	namespace := strings.TrimSpace(*hpaNamespace)
	deleteNamespace := namespace == ""
	if deleteNamespace {
		namespace = randomNamespaceName("hpa-autoscaling")
		if _, err := clientset.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}, metav1.CreateOptions{}); err != nil {
			t.Fatalf("ENVIRONMENT ERROR: Failed to create namespace: %v", err)
		}
		t.Cleanup(func() {
			if err := deleteNamespaceAndWait(context.Background(), t, clientset, namespace); err != nil {
				t.Errorf("CLEANUP FAILURE: %v. Please ensure this namespace is terminated manually to avoid resource leaks.", err)
			}
		})
	} else if _, err := clientset.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{}); err != nil {
		t.Fatalf("ENVIRONMENT ERROR: Failed to use pre-provisioned -hpa-namespace %s: %v", namespace, err)
	}
	t.Cleanup(func() {
		if err := cleanupHPAWorkload(context.Background(), clientset, namespace, workloadName, selector, claimTemplateName, mode); err != nil {
			t.Errorf("CLEANUP FAILURE for HPA workload in namespace %s: %v", namespace, err)
		}
	})

	if mode == allocationModeDRA {
		template := buildHPAResourceClaimTemplate(namespace, claimTemplateName, objectLabels, cfg)
		if _, err := clientset.ResourceV1().ResourceClaimTemplates(namespace).Create(ctx, template, metav1.CreateOptions{}); err != nil {
			t.Fatalf("ACCELERATOR WORKLOAD FAILURE: Could not create ResourceClaimTemplate: %v", err)
		}
	}
	deployment, err := buildHPADeployment(namespace, workloadName, objectLabels, mode, cfg, claimTemplateName)
	if err != nil {
		t.Fatalf("ACCELERATOR WORKLOAD FAILURE: Could not build accelerator Deployment: %v", err)
	}
	if _, err := clientset.AppsV1().Deployments(namespace).Create(ctx, deployment, metav1.CreateOptions{}); err != nil {
		t.Fatalf("ACCELERATOR WORKLOAD FAILURE: Could not create accelerator Deployment: %v", err)
	}
	pods, err := waitForDeploymentReplicas(ctx, clientset, namespace, workloadName, selector, hpaInitialReplicas, hpaScaleTimeout)
	if err != nil {
		t.Fatalf("ACCELERATOR WORKLOAD FAILURE: Initial accelerator-backed replica did not become Ready: %v", err)
	}
	if err := verifyDRAAllocations(ctx, clientset, pods, mode); err != nil {
		t.Fatalf("ACCELERATOR WORKLOAD FAILURE: %v", err)
	}
	initialPod := pods[0].Name

	// BumpMetric subtracts its delta when durationSec expires. The positive and
	// negative bumps live for seven days so the metric cannot revert mid-test.
	if err := bumpPodMetric(ctx, clientset, namespace, initialPod, metricName, hpaMetricHighValue); err != nil {
		t.Fatalf("METRICS PIPELINE FAILURE: Could not raise metric through the Pod proxy: %v", err)
	}
	high := *resource.NewQuantity(hpaMetricHighValue, resource.DecimalSI)
	if _, err := waitForPodMetrics(ctx, clientset, namespace, metricName, selector, *hpaMetricTimeout, func(values map[string]resource.Quantity) bool {
		value, ok := values[initialPod]
		return ok && value.Cmp(high) == 0
	}); err != nil {
		t.Fatalf("METRICS PIPELINE FAILURE: Metric %s never exposed the exact high gauge value; verify the adapter exposes the configured per-Pod gauge unchanged: %v", metricName, err)
	}

	hpa := buildAcceleratorHPA(namespace, workloadName, objectLabels, metricName)
	if _, err := clientset.AutoscalingV2().HorizontalPodAutoscalers(namespace).Create(ctx, hpa, metav1.CreateOptions{}); err != nil {
		t.Fatalf("HPA FAILURE: Could not create HorizontalPodAutoscaler: %v", err)
	}

	if _, err := waitForHPAReplicas(ctx, clientset, namespace, workloadName, hpaMaximumReplicas, hpaScaleTimeout); err != nil {
		t.Fatalf("HPA FAILURE: HPA did not request two replicas for the high metric: %v", err)
	}
	pods, err = waitForDeploymentReplicas(ctx, clientset, namespace, workloadName, selector, hpaMaximumReplicas, hpaScaleTimeout)
	if err != nil {
		if diagnostic, capacityShortfall := classifyAcceleratorCapacityShortfall(ctx, clientset, pods, mode, cfg); capacityShortfall {
			t.Fatalf("ENVIRONMENT ERROR: HPA requested two replicas, but the second accelerator-backed Pod could not be scheduled because runtime accelerator capacity was unavailable: %s: %v", diagnostic, err)
		}
		t.Fatalf("ACCELERATOR WORKLOAD FAILURE: HPA requested two replicas, but the second accelerator-backed Pod did not become Ready: %v", err)
	}
	if err := verifyDRAAllocations(ctx, clientset, pods, mode); err != nil {
		t.Fatalf("ACCELERATOR WORKLOAD FAILURE: %v", err)
	}

	secondPod := pods[0].Name
	if secondPod == initialPod {
		secondPod = pods[1].Name
	}
	// The second zero series makes the final all-zero assertion non-vacuous by
	// proving that the custom metrics API represented both scaled Pods.
	if err := bumpPodMetric(ctx, clientset, namespace, secondPod, metricName, 0); err != nil {
		t.Fatalf("METRICS PIPELINE FAILURE: Could not initialize the second Pod's metric: %v", err)
	}
	zero := resource.MustParse("0")
	if _, err := waitForPodMetrics(ctx, clientset, namespace, metricName, selector, *hpaMetricTimeout, func(values map[string]resource.Quantity) bool {
		first, firstOK := values[initialPod]
		second, secondOK := values[secondPod]
		return firstOK && secondOK && first.Cmp(high) == 0 && second.Cmp(zero) == 0
	}); err != nil {
		t.Fatalf("METRICS PIPELINE FAILURE: Adapter did not expose both scaled workload Pods: %v", err)
	}
	t.Logf("PASS: HPA scaled accelerator-backed workload from one to two replicas")

	if err := bumpPodMetric(ctx, clientset, namespace, initialPod, metricName, -hpaMetricHighValue); err != nil {
		t.Fatalf("METRICS PIPELINE FAILURE: Could not lower metric through the Pod proxy: %v", err)
	}
	if _, err := waitForPodMetrics(ctx, clientset, namespace, metricName, selector, *hpaMetricTimeout, func(values map[string]resource.Quantity) bool {
		initial, initialOK := values[initialPod]
		second, secondOK := values[secondPod]
		return initialOK && secondOK && initial.Cmp(zero) == 0 && second.Cmp(zero) == 0
	}); err != nil {
		t.Fatalf("METRICS PIPELINE FAILURE: Both scaled Pod metrics did not return to zero: %v", err)
	}
	if err := waitForStableScaleDown(ctx, clientset, namespace, workloadName, selector, hpaStabilityWindow, hpaScaleTimeout); err != nil {
		t.Fatalf("HPA FAILURE: Workload did not remain at one desired and Ready replica: %v", err)
	}
	t.Logf("PASS: HPA returned accelerator-backed workload to one replica")
}

func buildHPAResourceClaimTemplate(namespace, name string, objectLabels map[string]string, cfg AcceleratorConfig) *resourcev1.ResourceClaimTemplate {
	return &resourcev1.ResourceClaimTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: copyLabels(objectLabels)},
		Spec: resourcev1.ResourceClaimTemplateSpec{
			Spec: resourcev1.ResourceClaimSpec{
				Devices: resourcev1.DeviceClaim{Requests: []resourcev1.DeviceRequest{{
					Name: testRequestName,
					Exactly: &resourcev1.ExactDeviceRequest{
						DeviceClassName: cfg.DeviceClass,
						Count:           requestedAcceleratorCount,
					},
				}}},
			},
		},
	}
}

func buildHPADeployment(namespace, name string, podLabels map[string]string, mode string, cfg AcceleratorConfig, claimTemplateName string) (*appsv1.Deployment, error) {
	pod, err := buildTestPod(namespace, "", []corev1.Container{{
		Name:    "resource-consumer",
		Image:   hpaResourceConsumerImage,
		Command: []string{"/consumer"},
		Args:    []string{"-port=" + strconv.Itoa(int(hpaResourceConsumerPort))},
		Ports:   []corev1.ContainerPort{{Name: "metrics", ContainerPort: hpaResourceConsumerPort}},
		ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
			Path: "/metrics", Port: intstr.FromString("metrics"),
		}}, PeriodSeconds: 2},
	}}, testPodConfig{grantAccelerator: true, mode: mode, cfg: cfg})
	if err != nil {
		return nil, err
	}
	if mode == allocationModeDRA {
		if len(pod.Spec.ResourceClaims) != 1 {
			return nil, fmt.Errorf("DRA workload has %d Pod ResourceClaims, want 1", len(pod.Spec.ResourceClaims))
		}
		pod.Spec.ResourceClaims[0].ResourceClaimTemplateName = &claimTemplateName
	}
	replicas := hpaInitialReplicas
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: copyLabels(podLabels)},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{hpaRunLabelKey: podLabels[hpaRunLabelKey]}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: copyLabels(podLabels), Annotations: map[string]string{
					"prometheus.io/scrape": "true", "prometheus.io/port": strconv.Itoa(int(hpaResourceConsumerPort)), "prometheus.io/path": "/metrics",
				}},
				Spec: pod.Spec,
			},
		},
	}, nil
}

func buildAcceleratorHPA(namespace, name string, objectLabels map[string]string, metricName string) *autoscalingv2.HorizontalPodAutoscaler {
	minReplicas := hpaInitialReplicas
	stabilization := hpaScaleDownStabilization
	return &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: copyLabels(objectLabels)},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "Deployment", Name: name},
			MinReplicas:    &minReplicas,
			MaxReplicas:    hpaMaximumReplicas,
			Metrics: []autoscalingv2.MetricSpec{{
				Type: autoscalingv2.PodsMetricSourceType,
				Pods: &autoscalingv2.PodsMetricSource{
					Metric: autoscalingv2.MetricIdentifier{Name: metricName},
					Target: autoscalingv2.MetricTarget{Type: autoscalingv2.AverageValueMetricType, AverageValue: resource.NewQuantity(hpaMetricTargetValue, resource.DecimalSI)},
				},
			}},
			Behavior: &autoscalingv2.HorizontalPodAutoscalerBehavior{ScaleDown: &autoscalingv2.HPAScalingRules{StabilizationWindowSeconds: &stabilization}},
		},
	}
}

func bumpPodMetric(ctx context.Context, c kubernetes.Interface, namespace, podName, metricName string, delta int64) error {
	requestCtx, cancel := context.WithTimeout(ctx, hpaPodProxyTimeout)
	defer cancel()
	_, err := c.CoreV1().RESTClient().Post().Namespace(namespace).Resource("pods").Name(podName).SubResource("proxy").Suffix("BumpMetric").
		Param("metric", metricName).Param("delta", strconv.FormatInt(delta, 10)).
		Param("durationSec", strconv.FormatInt(int64(hpaMetricBumpLifetime/time.Second), 10)).MaxRetries(0).DoRaw(requestCtx)
	if err != nil {
		return fmt.Errorf("Pod proxy request failed: %w", err)
	}
	return nil
}

type podMetricValueList struct {
	Items []struct {
		DescribedObject corev1.ObjectReference `json:"describedObject"`
		MetricName      string                 `json:"metricName"`
		Value           resource.Quantity      `json:"value"`
	} `json:"items"`
}

func getPodMetrics(ctx context.Context, c kubernetes.Interface, namespace, metricName, selector string) (map[string]resource.Quantity, error) {
	path := fmt.Sprintf("/apis/custom.metrics.k8s.io/v1beta1/namespaces/%s/pods/*/%s", namespace, metricName)
	data, err := c.Discovery().RESTClient().Get().AbsPath(path).Param("labelSelector", selector).DoRaw(ctx)
	if err != nil {
		return nil, err
	}
	return parsePodMetrics(data, metricName)
}

func parsePodMetrics(data []byte, metricName string) (map[string]resource.Quantity, error) {
	var list podMetricValueList
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, err
	}
	values := make(map[string]resource.Quantity, len(list.Items))
	for _, item := range list.Items {
		if item.MetricName != metricName || item.DescribedObject.Name == "" {
			return nil, fmt.Errorf("unexpected custom metric item for %q: metric=%q object=%q", metricName, item.MetricName, item.DescribedObject.Name)
		}
		if item.DescribedObject.Kind != "" && !strings.EqualFold(item.DescribedObject.Kind, "Pod") {
			return nil, fmt.Errorf("custom metric describes %s %s, want Pod", item.DescribedObject.Kind, item.DescribedObject.Name)
		}
		if _, exists := values[item.DescribedObject.Name]; exists {
			return nil, fmt.Errorf("duplicate custom metric value for Pod %s", item.DescribedObject.Name)
		}
		values[item.DescribedObject.Name] = item.Value
	}
	return values, nil
}

func waitForPodMetrics(ctx context.Context, c kubernetes.Interface, namespace, metricName, selector string, timeout time.Duration, matches func(map[string]resource.Quantity) bool) (map[string]resource.Quantity, error) {
	var latest map[string]resource.Quantity
	var lastErr error
	err := wait.PollUntilContextTimeout(ctx, hpaPollInterval, timeout, true, func(ctx context.Context) (bool, error) {
		values, err := getPodMetrics(ctx, c, namespace, metricName, selector)
		if err != nil {
			lastErr = err
			if apierrors.IsNotFound(err) || isRetryableAPIError(err) {
				return false, nil
			}
			return false, err
		}
		lastErr = nil
		latest = values
		return matches(values), nil
	})
	if err != nil {
		return latest, fmt.Errorf("timed out after %s; latest=%s%s: %w", timeout, formatMetricValues(latest), lastAPIErrorSuffix(lastErr), err)
	}
	return latest, nil
}

func waitForHPAReplicas(ctx context.Context, c kubernetes.Interface, namespace, name string, want int32, timeout time.Duration) (*autoscalingv2.HorizontalPodAutoscaler, error) {
	var latest *autoscalingv2.HorizontalPodAutoscaler
	var lastErr error
	err := wait.PollUntilContextTimeout(ctx, hpaPollInterval, timeout, true, func(ctx context.Context) (bool, error) {
		hpa, err := c.AutoscalingV2().HorizontalPodAutoscalers(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			lastErr = err
			if isRetryableAPIError(err) {
				return false, nil
			}
			return false, err
		}
		lastErr = nil
		latest = hpa
		return hpa.Status.DesiredReplicas == want && hpaConditionTrue(hpa, autoscalingv2.ScalingActive), nil
	})
	if err != nil {
		return latest, fmt.Errorf("timed out after %s waiting for desiredReplicas=%d; latest=%s%s: %w", timeout, want, formatHPAStatus(latest), lastAPIErrorSuffix(lastErr), err)
	}
	return latest, nil
}

func waitForDeploymentReplicas(ctx context.Context, c kubernetes.Interface, namespace, name, selector string, want int32, timeout time.Duration) ([]corev1.Pod, error) {
	var deployment *appsv1.Deployment
	var pods []corev1.Pod
	var lastErr error
	err := wait.PollUntilContextTimeout(ctx, hpaPollInterval, timeout, true, func(ctx context.Context) (bool, error) {
		current, err := c.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			lastErr = err
			if isRetryableAPIError(err) {
				return false, nil
			}
			return false, err
		}
		list, err := c.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
		if err != nil {
			lastErr = err
			if isRetryableAPIError(err) {
				return false, nil
			}
			return false, err
		}
		lastErr = nil
		deployment = current
		pods = list.Items
		ready := readyPods(pods)
		return deployment.Status.ObservedGeneration >= deployment.Generation && deployment.Status.ReadyReplicas == want && int32(len(ready)) == want, nil
	})
	if err != nil {
		return pods, fmt.Errorf("timed out after %s waiting for %d Ready replicas; deployment=%s pods=%s%s: %w", timeout, want, formatDeploymentStatus(deployment), formatPodStatuses(pods), lastAPIErrorSuffix(lastErr), err)
	}
	return readyPods(pods), nil
}

func waitForStableScaleDown(ctx context.Context, c kubernetes.Interface, namespace, name, selector string, stableFor, timeout time.Duration) error {
	var stableSince time.Time
	var hpa *autoscalingv2.HorizontalPodAutoscaler
	var deployment *appsv1.Deployment
	var pods []corev1.Pod
	var lastErr error
	err := wait.PollUntilContextTimeout(ctx, hpaPollInterval, timeout, true, func(ctx context.Context) (bool, error) {
		currentHPA, err := c.AutoscalingV2().HorizontalPodAutoscalers(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			lastErr = err
			stableSince = time.Time{}
			if isRetryableAPIError(err) {
				return false, nil
			}
			return false, err
		}
		currentDeployment, err := c.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			lastErr = err
			stableSince = time.Time{}
			if isRetryableAPIError(err) {
				return false, nil
			}
			return false, err
		}
		list, err := c.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
		if err != nil {
			lastErr = err
			stableSince = time.Time{}
			if isRetryableAPIError(err) {
				return false, nil
			}
			return false, err
		}
		lastErr = nil
		hpa = currentHPA
		deployment = currentDeployment
		pods = list.Items
		ready := readyPods(pods)
		matches := hpa.Status.DesiredReplicas == hpaInitialReplicas && hpaConditionTrue(hpa, autoscalingv2.ScalingActive) &&
			deployment.Status.Replicas == hpaInitialReplicas && deployment.Status.ReadyReplicas == hpaInitialReplicas &&
			len(ready) == int(hpaInitialReplicas)
		if !matches {
			stableSince = time.Time{}
			return false, nil
		}
		if stableSince.IsZero() {
			stableSince = time.Now()
		}
		return time.Since(stableSince) >= stableFor, nil
	})
	if err != nil {
		return fmt.Errorf("scale-down was not stable for %s; hpa=%s deployment=%s pods=%s%s: %w", stableFor, formatHPAStatus(hpa), formatDeploymentStatus(deployment), formatPodStatuses(pods), lastAPIErrorSuffix(lastErr), err)
	}
	return nil
}

func verifyDRAAllocations(ctx context.Context, c kubernetes.Interface, pods []corev1.Pod, mode string) error {
	if mode != allocationModeDRA {
		return nil
	}
	for i := range pods {
		pod := &pods[i]
		if len(pod.Status.ResourceClaimStatuses) != 1 || pod.Status.ResourceClaimStatuses[0].ResourceClaimName == nil {
			return fmt.Errorf("Pod %s does not report one generated ResourceClaim", pod.Name)
		}
		claimName := *pod.Status.ResourceClaimStatuses[0].ResourceClaimName
		claim, err := c.ResourceV1().ResourceClaims(pod.Namespace).Get(ctx, claimName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to get ResourceClaim %s for Pod %s: %w", claimName, pod.Name, err)
		}
		if claim.Status.Allocation == nil {
			return fmt.Errorf("ResourceClaim %s for Pod %s is not allocated", claimName, pod.Name)
		}
	}
	return nil
}

func classifyAcceleratorCapacityShortfall(ctx context.Context, c kubernetes.Interface, pods []corev1.Pod, mode string, cfg AcceleratorConfig) (string, bool) {
	var pending *corev1.Pod
	for i := range pods {
		pod := &pods[i]
		if pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodRunning && podConditionTrue(pod, corev1.PodReady) {
			continue
		}
		if pending != nil {
			return "", false
		}
		pending = pod
	}
	if len(readyPods(pods)) != 1 || pending == nil {
		return "", false
	}
	ok, message := podUnschedulable(pending)
	if !ok {
		return "", false
	}
	diagnostic := fmt.Sprintf("Pod %s Unschedulable: %s", pending.Name, message)
	if mode == allocationModeDevicePlugin {
		if !strings.Contains(message, "Insufficient "+cfg.ExtendedResource) {
			return "", false
		}
		return diagnostic, true
	}
	if mode != allocationModeDRA || !strings.Contains(message, "cannot allocate all claims") {
		return "", false
	}
	claimCtx, cancel := context.WithTimeout(ctx, hpaCapacityCheckTimeout)
	defer cancel()
	claimNames, err := podGeneratedClaims(claimCtx, c, pending.Namespace, pending.Name, pending)
	if err != nil || len(claimNames) != 1 {
		return "", false
	}
	claim, err := c.ResourceV1().ResourceClaims(pending.Namespace).Get(claimCtx, claimNames[0], metav1.GetOptions{})
	if err != nil || claim.Status.Allocation != nil {
		return "", false
	}
	return fmt.Sprintf("%s; generated ResourceClaim %s is unallocated", diagnostic, claim.Name), true
}

func cleanupHPAWorkload(ctx context.Context, c kubernetes.Interface, namespace, name, selector, claimTemplateName, mode string) error {
	cleanupCtx, cancel := context.WithTimeout(ctx, hpaCleanupTimeout)
	defer cancel()
	var errs []error
	if err := c.AutoscalingV2().HorizontalPodAutoscalers(namespace).Delete(cleanupCtx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		errs = append(errs, fmt.Errorf("delete HPA: %w", err))
	}
	foreground := metav1.DeletePropagationForeground
	if err := c.AppsV1().Deployments(namespace).Delete(cleanupCtx, name, metav1.DeleteOptions{PropagationPolicy: &foreground}); err != nil && !apierrors.IsNotFound(err) {
		errs = append(errs, fmt.Errorf("delete Deployment: %w", err))
	}
	pods, err := c.CoreV1().Pods(namespace).List(cleanupCtx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		errs = append(errs, fmt.Errorf("list workload Pods: %w", err))
	} else {
		for i := range pods.Items {
			pod := &pods.Items[i]
			if err := deletePodAndWait(cleanupCtx, c, namespace, pod.Name, pod); err != nil {
				errs = append(errs, fmt.Errorf("delete Pod %s: %w", pod.Name, err))
			}
		}
	}
	if mode == allocationModeDRA {
		if err := waitForHPAClaimsGone(cleanupCtx, c, namespace, name); err != nil {
			errs = append(errs, err)
		}
		if err := c.ResourceV1().ResourceClaimTemplates(namespace).Delete(cleanupCtx, claimTemplateName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("delete ResourceClaimTemplate: %w", err))
		}
	}
	return errors.Join(errs...)
}

func waitForHPAClaimsGone(ctx context.Context, c kubernetes.Interface, namespace, workloadName string) error {
	err := wait.PollUntilContextTimeout(ctx, 2*time.Second, hpaCleanupTimeout, true, func(ctx context.Context) (bool, error) {
		claims, err := c.ResourceV1().ResourceClaims(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			if isRetryableAPIError(err) {
				return false, nil
			}
			return false, err
		}
		for _, claim := range claims.Items {
			if strings.HasPrefix(claim.Name, workloadName+"-") && claim.Status.Allocation != nil {
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
		return fmt.Errorf("wait for generated ResourceClaims to be removed or deallocated: %w", err)
	}
	return nil
}

func readyPods(items []corev1.Pod) []corev1.Pod {
	ready := make([]corev1.Pod, 0, len(items))
	for _, pod := range items {
		if pod.DeletionTimestamp == nil && pod.Status.Phase == corev1.PodRunning && podConditionTrue(&pod, corev1.PodReady) {
			ready = append(ready, pod)
		}
	}
	sort.Slice(ready, func(i, j int) bool { return ready[i].Name < ready[j].Name })
	return ready
}

func podConditionTrue(pod *corev1.Pod, conditionType corev1.PodConditionType) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == conditionType {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func hpaConditionTrue(hpa *autoscalingv2.HorizontalPodAutoscaler, conditionType autoscalingv2.HorizontalPodAutoscalerConditionType) bool {
	for _, condition := range hpa.Status.Conditions {
		if condition.Type == conditionType {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func formatMetricValues(values map[string]resource.Quantity) string {
	parts := make([]string, 0, len(values))
	for name, value := range values {
		parts = append(parts, fmt.Sprintf("%s=%s", name, value.String()))
	}
	sort.Strings(parts)
	return "{" + strings.Join(parts, ", ") + "}"
}

func formatHPAStatus(hpa *autoscalingv2.HorizontalPodAutoscaler) string {
	if hpa == nil {
		return "<none>"
	}
	conditions := make([]string, 0, len(hpa.Status.Conditions))
	for _, condition := range hpa.Status.Conditions {
		conditions = append(conditions, fmt.Sprintf("%s=%s(%s:%s)", condition.Type, condition.Status, condition.Reason, condition.Message))
	}
	return fmt.Sprintf("current=%d desired=%d conditions=[%s]", hpa.Status.CurrentReplicas, hpa.Status.DesiredReplicas, strings.Join(conditions, "; "))
}

func formatDeploymentStatus(deployment *appsv1.Deployment) string {
	if deployment == nil {
		return "<none>"
	}
	return fmt.Sprintf("current=%d updated=%d ready=%d available=%d unavailable=%d", deployment.Status.Replicas,
		deployment.Status.UpdatedReplicas, deployment.Status.ReadyReplicas, deployment.Status.AvailableReplicas, deployment.Status.UnavailableReplicas)
}

func formatPodStatuses(pods []corev1.Pod) string {
	parts := make([]string, 0, len(pods))
	for _, pod := range pods {
		reasons := []string{}
		for _, condition := range pod.Status.Conditions {
			if condition.Status != corev1.ConditionTrue && condition.Reason != "" {
				reasons = append(reasons, condition.Reason+":"+condition.Message)
			}
		}
		parts = append(parts, fmt.Sprintf("%s phase=%s node=%s reasons=[%s]", pod.Name, pod.Status.Phase, pod.Spec.NodeName, strings.Join(reasons, "; ")))
	}
	sort.Strings(parts)
	return "[" + strings.Join(parts, ", ") + "]"
}

func copyLabels(input map[string]string) map[string]string {
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}
