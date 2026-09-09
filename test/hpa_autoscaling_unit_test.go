package conformance

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"
)

func TestBuildHPADeployment(t *testing.T) {
	cfg := nvidiaConfig(t)
	podLabels := map[string]string{hpaTestLabelKey: hpaTestLabelValue, hpaRunLabelKey: "run1"}

	t.Run("device plugin", func(t *testing.T) {
		deployment, err := buildHPADeployment("ns", "workload", podLabels, allocationModeDevicePlugin, cfg, "unused-template")
		if err != nil {
			t.Fatalf("buildHPADeployment unexpected error: %v", err)
		}
		verifyHPADeploymentCommon(t, deployment, podLabels)
		resourceName := corev1.ResourceName(cfg.ExtendedResource)
		quantity := deployment.Spec.Template.Spec.Containers[0].Resources.Limits[resourceName]
		if quantity.Cmp(resource.MustParse("1")) != 0 {
			t.Fatalf("accelerator limit = %s, want 1", quantity.String())
		}
		if len(deployment.Spec.Template.Spec.ResourceClaims) != 0 {
			t.Fatalf("device-plugin deployment has ResourceClaims: %v", deployment.Spec.Template.Spec.ResourceClaims)
		}
	})

	t.Run("DRA", func(t *testing.T) {
		deployment, err := buildHPADeployment("ns", "workload", podLabels, allocationModeDRA, cfg, "run-claim-template")
		if err != nil {
			t.Fatalf("buildHPADeployment unexpected error: %v", err)
		}
		verifyHPADeploymentCommon(t, deployment, podLabels)
		claims := deployment.Spec.Template.Spec.ResourceClaims
		if len(claims) != 1 || claims[0].ResourceClaimTemplateName == nil || *claims[0].ResourceClaimTemplateName != "run-claim-template" {
			t.Fatalf("resource claims = %v, want run-claim-template", claims)
		}
		containerClaims := deployment.Spec.Template.Spec.Containers[0].Resources.Claims
		if len(containerClaims) != 1 || containerClaims[0].Name != claims[0].Name {
			t.Fatalf("container claims = %v, Pod claims = %v", containerClaims, claims)
		}
		template := buildHPAResourceClaimTemplate("ns", "run-claim-template", podLabels, cfg)
		request := template.Spec.Spec.Devices.Requests[0]
		if template.Name != "run-claim-template" || template.Labels[hpaTestLabelKey] != hpaTestLabelValue || template.Labels[hpaRunLabelKey] != "run1" || request.Exactly == nil || request.Exactly.DeviceClassName != cfg.DeviceClass || request.Exactly.Count != requestedAcceleratorCount {
			t.Fatalf("ResourceClaimTemplate = %+v", template)
		}
	})
}

func verifyHPADeploymentCommon(t *testing.T, deployment *appsv1.Deployment, podLabels map[string]string) {
	t.Helper()
	if deployment.Name != "workload" || deployment.Namespace != "ns" || deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != hpaInitialReplicas {
		t.Fatalf("deployment metadata or replicas = %+v", deployment)
	}
	if deployment.Spec.Template.Spec.TerminationGracePeriodSeconds != nil {
		t.Fatalf("termination grace override = %v, want default", deployment.Spec.Template.Spec.TerminationGracePeriodSeconds)
	}
	if len(deployment.Spec.Selector.MatchLabels) != 1 || deployment.Spec.Selector.MatchLabels[hpaRunLabelKey] != "run1" {
		t.Fatalf("selector labels = %v, want run label only", deployment.Spec.Selector.MatchLabels)
	}
	for key, value := range podLabels {
		if deployment.Labels[key] != value || deployment.Spec.Template.Labels[key] != value {
			t.Fatalf("label %s not copied to Deployment and Pod template", key)
		}
	}
	annotations := deployment.Spec.Template.Annotations
	if annotations["prometheus.io/scrape"] != "true" || annotations["prometheus.io/port"] != "8080" || annotations["prometheus.io/path"] != "/metrics" {
		t.Fatalf("scrape annotations = %v", annotations)
	}
	container := deployment.Spec.Template.Spec.Containers[0]
	if container.Image != hpaResourceConsumerImage || len(container.Command) != 1 || container.Command[0] != "/consumer" || container.ReadinessProbe == nil {
		t.Fatalf("resource-consumer container = %+v", container)
	}
}

func TestParsePodMetrics(t *testing.T) {
	values, err := parsePodMetrics([]byte(`{"items":[{"describedObject":{"kind":"Pod","name":"pod-a"},"metricName":"queue_depth","value":"100"},{"describedObject":{"kind":"Pod","name":"pod-b"},"metricName":"queue_depth","value":"0"}]}`), "queue_depth")
	if err != nil {
		t.Fatalf("parsePodMetrics unexpected error: %v", err)
	}
	if value := values["pod-a"]; value.Cmp(resource.MustParse("100")) != 0 {
		t.Fatalf("value = %s, want 100", value.String())
	}
	if value, ok := values["pod-b"]; !ok || value.Cmp(resource.MustParse("0")) != 0 {
		t.Fatalf("value = %s, present = %t, want explicit zero", value.String(), ok)
	}

	for _, tc := range []struct {
		name string
		data string
	}{
		{name: "invalid JSON", data: `{`},
		{name: "metric mismatch", data: `{"items":[{"describedObject":{"kind":"Pod","name":"pod-a"},"metricName":"other","value":"1"}]}`},
		{name: "missing Pod name", data: `{"items":[{"describedObject":{"kind":"Pod"},"metricName":"queue_depth","value":"1"}]}`},
		{name: "wrong kind", data: `{"items":[{"describedObject":{"kind":"Service","name":"svc"},"metricName":"queue_depth","value":"1"}]}`},
		{name: "missing value", data: `{"items":[{"describedObject":{"kind":"Pod","name":"pod-a"},"metricName":"queue_depth"}]}`},
		{name: "null value", data: `{"items":[{"describedObject":{"kind":"Pod","name":"pod-a"},"metricName":"queue_depth","value":null}]}`},
		{name: "duplicate Pod", data: `{"items":[{"describedObject":{"kind":"Pod","name":"pod-a"},"metricName":"queue_depth","value":"1"},{"describedObject":{"kind":"Pod","name":"pod-a"},"metricName":"queue_depth","value":"2"}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parsePodMetrics([]byte(tc.data), "queue_depth"); err == nil {
				t.Fatal("parsePodMetrics expected error")
			}
		})
	}
}

func TestClassifyAcceleratorCapacityShortfall(t *testing.T) {
	cfg := nvidiaConfig(t)
	readyPod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "ready", Namespace: "ns"},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}

	t.Run("device-plugin capacity shortfall", func(t *testing.T) {
		message := "0/1 nodes are available: 1 Insufficient " + cfg.ExtendedResource
		pendingPod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pending", Namespace: "ns"}, Status: corev1.PodStatus{Phase: corev1.PodPending, Conditions: []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable, Message: message}}}}
		diagnostic, ok, err := classifyAcceleratorCapacityShortfall(t.Context(), fake.NewClientset(&readyPod, &pendingPod), "ns", "", allocationModeDevicePlugin, cfg)
		want := "Pod pending Unschedulable: " + message
		if err != nil || !ok || diagnostic != want {
			t.Fatalf("classification = %t, diagnostic = %q, err = %v, want %q", ok, diagnostic, err, want)
		}
	})

	t.Run("DRA capacity shortfall", func(t *testing.T) {
		claimName := "pending-claim"
		message := "0/1 nodes are available: cannot allocate all claims"
		pendingPod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pending", Namespace: "ns"}, Status: corev1.PodStatus{Phase: corev1.PodPending, Conditions: []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable, Message: message}}, ResourceClaimStatuses: []corev1.PodResourceClaimStatus{{Name: testRequestName, ResourceClaimName: &claimName}}}}
		claim := resourcev1.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Name: claimName, Namespace: "ns"}}
		diagnostic, ok, err := classifyAcceleratorCapacityShortfall(t.Context(), fake.NewClientset(&readyPod, &pendingPod, &claim), "ns", "", allocationModeDRA, cfg)
		want := "Pod pending Unschedulable: " + message + "; generated ResourceClaim " + claimName + " is unallocated"
		if err != nil || !ok || diagnostic != want {
			t.Fatalf("classification = %t, diagnostic = %q, err = %v, want %q", ok, diagnostic, err, want)
		}
	})

	t.Run("genuine DRA driver failure", func(t *testing.T) {
		claimName := "pending-claim"
		pendingPod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pending", Namespace: "ns"}, Status: corev1.PodStatus{Phase: corev1.PodPending, Conditions: []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable, Message: "DRA driver failed to prepare the claim"}}, ResourceClaimStatuses: []corev1.PodResourceClaimStatus{{Name: testRequestName, ResourceClaimName: &claimName}}}}
		claim := resourcev1.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Name: claimName, Namespace: "ns"}}
		if diagnostic, ok, err := classifyAcceleratorCapacityShortfall(t.Context(), fake.NewClientset(&readyPod, &pendingPod, &claim), "ns", "", allocationModeDRA, cfg); err != nil || ok {
			t.Fatalf("unexpected classification: %q, err = %v", diagnostic, err)
		}
	})

	t.Run("unrelated Unschedulable reason", func(t *testing.T) {
		pendingPod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pending", Namespace: "ns"}, Status: corev1.PodStatus{Phase: corev1.PodPending, Conditions: []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable, Message: "0/1 nodes are available: 1 node had an untolerated taint"}}}}
		if diagnostic, ok, err := classifyAcceleratorCapacityShortfall(t.Context(), fake.NewClientset(&readyPod, &pendingPod), "ns", "", allocationModeDevicePlugin, cfg); err != nil || ok {
			t.Fatalf("unexpected classification: %q, err = %v", diagnostic, err)
		}
	})

	t.Run("ignores other runs", func(t *testing.T) {
		_, ready, pending := hpaReplicaFixtures()
		foreign := ready.DeepCopy()
		foreign.Name = "foreign"
		foreign.Labels[hpaRunLabelKey] = "other"
		client := fake.NewClientset(ready, pending, foreign)
		if diagnostic, ok, err := classifyAcceleratorCapacityShortfall(t.Context(), client, "ns", hpaRunLabelKey+"=run1", allocationModeDevicePlugin, cfg); err != nil || !ok {
			t.Fatalf("classification = %t, diagnostic = %q, err = %v", ok, diagnostic, err)
		}
	})

	for _, resourceName := range []string{"pods", "resourceclaims"} {
		t.Run(resourceName+" read failure", func(t *testing.T) {
			_, ready, pending := hpaReplicaFixtures()
			claimName := "pending-claim"
			pending.Status.Conditions[0].Message = "cannot allocate all claims"
			pending.Status.ResourceClaimStatuses = []corev1.PodResourceClaimStatus{{Name: testRequestName, ResourceClaimName: &claimName}}
			client := fake.NewClientset(ready, pending)
			readErr := apierrors.NewForbidden(schema.GroupResource{Resource: resourceName}, "", errors.New("denied"))
			client.PrependReactor("*", resourceName, func(k8stesting.Action) (bool, runtime.Object, error) {
				return true, nil, readErr
			})
			diagnostic, ok, err := classifyAcceleratorCapacityShortfall(t.Context(), client, "ns", "", allocationModeDRA, cfg)
			if ok || diagnostic != "" || !errors.Is(err, readErr) {
				t.Fatalf("classification = %t, diagnostic = %q, err = %v", ok, diagnostic, err)
			}
		})
	}
}

func TestBuildAcceleratorHPA(t *testing.T) {
	hpa := buildAcceleratorHPA("ns", "workload", nil, "queue_depth")
	spec := hpa.Spec
	if spec.MinReplicas == nil || *spec.MinReplicas != 1 || spec.MaxReplicas != 2 {
		t.Fatalf("replica bounds = %v, %d", spec.MinReplicas, spec.MaxReplicas)
	}
	if spec.ScaleTargetRef.APIVersion != "apps/v1" || spec.ScaleTargetRef.Kind != "Deployment" || spec.ScaleTargetRef.Name != "workload" {
		t.Fatalf("scale target = %+v", spec.ScaleTargetRef)
	}
	if len(spec.Metrics) != 1 || spec.Metrics[0].Type != autoscalingv2.PodsMetricSourceType || spec.Metrics[0].Pods == nil {
		t.Fatalf("metrics = %+v", spec.Metrics)
	}
	metric := spec.Metrics[0].Pods
	if metric.Metric.Name != "queue_depth" || metric.Target.Type != autoscalingv2.AverageValueMetricType ||
		metric.Target.AverageValue == nil || metric.Target.AverageValue.Cmp(resource.MustParse("10")) != 0 {
		t.Fatalf("Pod metric = %+v", metric)
	}
	if spec.Behavior == nil || spec.Behavior.ScaleDown == nil {
		t.Fatal("missing scale-down behavior")
	}
	down := spec.Behavior.ScaleDown
	if down.SelectPolicy == nil || *down.SelectPolicy != autoscalingv2.DisabledPolicySelect ||
		down.StabilizationWindowSeconds == nil || *down.StabilizationWindowSeconds != 60 {
		t.Fatalf("scale-down behavior = %+v", down)
	}
}

func TestEnableHPAScaleDown(t *testing.T) {
	t.Run("patches only the downscale policy", func(t *testing.T) {
		hpa := buildAcceleratorHPA("ns", "workload", nil, "queue_depth")
		client := fake.NewClientset(hpa)
		if err := enableHPAScaleDown(t.Context(), client, "ns", "workload"); err != nil {
			t.Fatal(err)
		}
		action, ok := client.Actions()[0].(k8stesting.PatchAction)
		if !ok || action.GetPatchType() != types.MergePatchType ||
			string(action.GetPatch()) != `{"spec":{"behavior":{"scaleDown":{"selectPolicy":"Max"}}}}` {
			t.Fatalf("unexpected patch action: %#v", client.Actions()[0])
		}
		got, err := client.AutoscalingV2().HorizontalPodAutoscalers("ns").Get(t.Context(), "workload", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		want := hpa.DeepCopy()
		policy := autoscalingv2.MaxChangePolicySelect
		want.Spec.Behavior.ScaleDown.SelectPolicy = &policy
		if !apiequality.Semantic.DeepEqual(got.Spec, want.Spec) {
			t.Fatalf("HPA spec = %+v, want %+v", got.Spec, want.Spec)
		}
	})

	t.Run("surfaces patch failure", func(t *testing.T) {
		client := fake.NewClientset()
		patchErr := apierrors.NewForbidden(schema.GroupResource{Resource: "horizontalpodautoscalers"}, "workload", errors.New("denied"))
		client.PrependReactor("patch", "horizontalpodautoscalers", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, patchErr
		})
		if err := enableHPAScaleDown(t.Context(), client, "ns", "workload"); !errors.Is(err, patchErr) {
			t.Fatalf("patch error = %v, want %v", err, patchErr)
		}
	})

	t.Run("cancels an in-flight request", func(t *testing.T) {
		started := make(chan struct{})
		release := make(chan struct{})
		client := hpaRESTClient(t, func(w http.ResponseWriter, r *http.Request) {
			close(started)
			select {
			case <-r.Context().Done():
			case <-release:
			}
		})
		t.Cleanup(func() { close(release) })
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		result := make(chan error, 1)
		go func() { result <- enableHPAScaleDown(ctx, client, "ns", "workload") }()
		select {
		case <-started:
			cancel()
		case <-time.After(5 * time.Second):
			t.Fatal("patch request did not start")
		}
		select {
		case err := <-result:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("patch error = %v, want context cancellation", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("patch did not honor cancellation")
		}
	})
}

func TestCleanupHPAWorkload(t *testing.T) {
	t.Run("waits for asynchronous Deployment deletion", func(t *testing.T) {
		deployment, _, replacement := hpaReplicaFixtures()
		client := fake.NewClientset(deployment, replacement)
		client.PrependReactor("delete", "deployments", func(action k8stesting.Action) (bool, runtime.Object, error) {
			options := action.(k8stesting.DeleteAction).GetDeleteOptions()
			if options.PropagationPolicy == nil || *options.PropagationPolicy != metav1.DeletePropagationForeground {
				t.Errorf("deletion propagation = %v, want foreground", options.PropagationPolicy)
			}
			// The API accepted deletion, but the dependent controller still exists.
			return true, nil, nil
		})
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		err := cleanupHPAWorkload(ctx, client, "ns", "workload", hpaRunLabelKey+"=run1", "template", allocationModeDevicePlugin)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("cleanup error = %v, want deadline", err)
		}
		for _, action := range client.Actions() {
			if action.Matches("delete", "pods") {
				t.Fatal("cleanup deleted a Pod while its controller was still active")
			}
		}
	})

	t.Run("waits for remaining Pods after Deployment deletion", func(t *testing.T) {
		_, ready, _ := hpaReplicaFixtures()
		client := fake.NewClientset(ready)
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		if err := cleanupHPAWorkload(ctx, client, "ns", "workload", hpaRunLabelKey+"=run1", "template", allocationModeDevicePlugin); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("cleanup error = %v, want deadline for remaining Pod", err)
		}
	})

	t.Run("preserves the supplied namespace and unrelated resources", func(t *testing.T) {
		deployment, unrelated, _ := hpaReplicaFixtures()
		unrelated.Labels[hpaRunLabelKey] = "other"
		namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns"}}
		hpa := buildAcceleratorHPA("ns", "workload", nil, "queue_depth")
		client := fake.NewClientset(deployment, hpa, namespace, unrelated)
		if err := cleanupHPAWorkload(t.Context(), client, "ns", "workload", hpaRunLabelKey+"=run1", "template", allocationModeDevicePlugin); err != nil {
			t.Fatal(err)
		}
		if _, err := client.CoreV1().Namespaces().Get(t.Context(), "ns", metav1.GetOptions{}); err != nil {
			t.Fatalf("supplied namespace was affected: %v", err)
		}
		if _, err := client.CoreV1().Pods("ns").Get(t.Context(), unrelated.Name, metav1.GetOptions{}); err != nil {
			t.Fatalf("unrelated Pod was affected: %v", err)
		}
		if _, err := client.AutoscalingV2().HorizontalPodAutoscalers("ns").Get(t.Context(), "workload", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			t.Fatalf("HPA remains after cleanup: %v", err)
		}
	})

	t.Run("surfaces permanent observation errors", func(t *testing.T) {
		client := fake.NewClientset()
		readErr := apierrors.NewForbidden(schema.GroupResource{Resource: "deployments"}, "workload", errors.New("denied"))
		client.PrependReactor("get", "deployments", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, readErr
		})
		if err := cleanupHPAWorkload(t.Context(), client, "ns", "workload", "", "template", allocationModeDevicePlugin); !errors.Is(err, readErr) {
			t.Fatalf("cleanup error = %v, want %v", err, readErr)
		}
	})

	t.Run("waits for DRA allocation release", func(t *testing.T) {
		template := buildHPAResourceClaimTemplate("ns", "template", nil, nvidiaConfig(t))
		client := fake.NewClientset(template)
		lists := 0
		client.PrependReactor("list", "resourceclaims", func(k8stesting.Action) (bool, runtime.Object, error) {
			lists++
			return true, &resourcev1.ResourceClaimList{Items: []resourcev1.ResourceClaim{*claim("ns", "workload-pod-claim", lists == 1)}}, nil
		})
		client.PrependReactor("delete", "resourceclaimtemplates", func(k8stesting.Action) (bool, runtime.Object, error) {
			if lists < 2 {
				t.Error("claim template deleted before allocation release")
			}
			return false, nil, nil
		})
		if err := cleanupHPAWorkload(t.Context(), client, "ns", "workload", "", "template", allocationModeDRA); err != nil {
			t.Fatal(err)
		}
		if lists < 2 {
			t.Fatalf("claim list calls = %d, want allocated then released", lists)
		}
	})
}

func TestWaitForHPAScaleUp(t *testing.T) {
	cfg := nvidiaConfig(t)

	t.Run("deadline with a current capacity shortfall", func(t *testing.T) {
		deployment, ready, pending := hpaReplicaFixtures()
		client := fake.NewClientset(deployment, ready, pending)
		_, err := waitForHPAScaleUp(t.Context(), client, "ns", "workload", hpaRunLabelKey+"=run1", allocationModeDevicePlugin, cfg, 50*time.Millisecond)
		if !errors.Is(err, context.DeadlineExceeded) || !strings.HasPrefix(err.Error(), "ENVIRONMENT ERROR:") {
			t.Fatalf("scale-up error = %v, want capacity evidence and deadline", err)
		}
	})

	t.Run("opaque rate-limiter error still reaches classification", func(t *testing.T) {
		deployment, ready, pending := hpaReplicaFixtures()
		client := fake.NewClientset(deployment, ready, pending)
		rateErr := errors.New("client rate limiter Wait returned an error: rate: Wait(n=1) would exceed context deadline")
		client.PrependReactor("get", "deployments", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, rateErr
		})
		_, err := waitForHPAScaleUp(t.Context(), client, "ns", "workload", hpaRunLabelKey+"=run1", allocationModeDevicePlugin, cfg, time.Second)
		if !errors.Is(err, rateErr) || !strings.HasPrefix(err.Error(), "ENVIRONMENT ERROR:") {
			t.Fatalf("scale-up error = %v, want capacity evidence and rate error", err)
		}
	})

	t.Run("historical shortage does not override fresh Ready Pods", func(t *testing.T) {
		deployment, ready, pending := hpaReplicaFixtures()
		client := fake.NewClientset(deployment, ready, pending)
		readErr := apierrors.NewForbidden(schema.GroupResource{Resource: "deployments"}, "workload", errors.New("denied"))
		gets := 0
		client.PrependReactor("get", "deployments", func(k8stesting.Action) (bool, runtime.Object, error) {
			gets++
			if gets == 1 {
				return false, nil, nil
			}
			pending.Status = ready.Status
			if err := client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), pending, "ns"); err != nil {
				t.Error(err)
			}
			return true, nil, readErr
		})
		_, err := waitForHPAScaleUp(t.Context(), client, "ns", "workload", hpaRunLabelKey+"=run1", allocationModeDevicePlugin, cfg, 2*hpaPollInterval)
		if !errors.Is(err, readErr) || !strings.HasPrefix(err.Error(), "ACCELERATOR WORKLOAD FAILURE:") {
			t.Fatalf("scale-up error = %v, want observation failure without stale capacity attribution", err)
		}
	})

	t.Run("preserves the readiness and diagnosis failures", func(t *testing.T) {
		client := fake.NewClientset()
		readErr := errors.New("readiness API failure")
		diagnosticErr := apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "", errors.New("denied"))
		client.PrependReactor("get", "deployments", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, readErr
		})
		client.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, diagnosticErr
		})
		_, err := waitForHPAScaleUp(t.Context(), client, "ns", "workload", "", allocationModeDevicePlugin, cfg, time.Second)
		if !errors.Is(err, readErr) || !errors.Is(err, diagnosticErr) || !strings.HasPrefix(err.Error(), "ACCELERATOR WORKLOAD FAILURE:") {
			t.Fatalf("scale-up error = %v, want both failures without capacity attribution", err)
		}
	})

	t.Run("returns both Ready Pods on success", func(t *testing.T) {
		deployment, ready, second := hpaReplicaFixtures()
		deployment.Status.ReadyReplicas = 2
		second.Status = ready.Status
		client := fake.NewClientset(deployment, ready, second)
		pods, err := waitForHPAScaleUp(t.Context(), client, "ns", "workload", "", allocationModeDevicePlugin, cfg, time.Second)
		if err != nil || len(pods) != 2 {
			t.Fatalf("pods = %v, err = %v", pods, err)
		}
	})
}

func TestWaitForDeploymentReplicas(t *testing.T) {
	t.Run("returns raw Pods on failure", func(t *testing.T) {
		deployment, ready, pending := hpaReplicaFixtures()
		client := fake.NewClientset(deployment, ready, pending)
		pods, err := waitForDeploymentReplicas(t.Context(), client, "ns", "workload", "", 2, 50*time.Millisecond)
		if !errors.Is(err, context.DeadlineExceeded) || len(pods) != 2 || len(readyPods(pods)) != 1 {
			t.Fatalf("pods = %v, err = %v, want raw readiness snapshot", pods, err)
		}
	})

	t.Run("returns only Ready Pods on success", func(t *testing.T) {
		deployment, ready, pending := hpaReplicaFixtures()
		client := fake.NewClientset(deployment, ready, pending)
		pods, err := waitForDeploymentReplicas(t.Context(), client, "ns", "workload", "", 1, time.Second)
		if err != nil || len(pods) != 1 || pods[0].Name != ready.Name {
			t.Fatalf("pods = %v, err = %v, want only Ready Pod", pods, err)
		}
	})

	t.Run("does not call a permanent error a timeout", func(t *testing.T) {
		client := fake.NewClientset()
		readErr := apierrors.NewForbidden(schema.GroupResource{Resource: "deployments"}, "workload", errors.New("denied"))
		client.PrependReactor("get", "deployments", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, readErr
		})
		_, err := waitForDeploymentReplicas(t.Context(), client, "ns", "workload", "", 1, time.Minute)
		if !errors.Is(err, readErr) || strings.Contains(err.Error(), "timed out after") {
			t.Fatalf("readiness error = %v, want accurate permanent failure", err)
		}
	})
}

func TestWaitForHPAReplicas(t *testing.T) {
	client := fake.NewClientset()
	readErr := apierrors.NewForbidden(schema.GroupResource{Resource: "horizontalpodautoscalers"}, "workload", errors.New("denied"))
	client.PrependReactor("get", "horizontalpodautoscalers", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, readErr
	})
	_, err := waitForHPAReplicas(t.Context(), client, "ns", "workload", 2, time.Minute)
	if !errors.Is(err, readErr) || strings.Contains(err.Error(), "timed out after") {
		t.Fatalf("HPA error = %v, want accurate permanent failure", err)
	}
}

func TestWaitForPodMetrics(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{name: "Forbidden", status: http.StatusForbidden, body: `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","message":"denied","code":403}`},
		{name: "malformed response", status: http.StatusOK, body: `{`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int32
			client := hpaRESTClient(t, func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if r.URL.Path != "/apis/custom.metrics.k8s.io/v1beta1/namespaces/ns/pods/*/queue_depth" ||
					r.URL.Query().Get("labelSelector") != hpaRunLabelKey+"=run1" {
					t.Errorf("unexpected metric URL: %s", r.URL)
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				fmt.Fprint(w, tc.body)
			})
			_, err := waitForPodMetrics(t.Context(), client, "ns", "queue_depth", hpaRunLabelKey+"=run1", time.Minute, func(map[string]resource.Quantity) bool {
				t.Error("predicate called after an invalid metric response")
				return true
			})
			if err == nil || strings.Contains(err.Error(), "timed out after") || requests.Load() != 1 {
				t.Fatalf("metric error = %v, requests = %d", err, requests.Load())
			}
			if tc.status == http.StatusForbidden && !apierrors.IsForbidden(err) {
				t.Fatalf("Forbidden cause was lost: %v", err)
			}
		})
	}
}

func hpaReplicaFixtures() (*appsv1.Deployment, *corev1.Pod, *corev1.Pod) {
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "workload", Namespace: "ns", Generation: 1},
		Status:     appsv1.DeploymentStatus{ObservedGeneration: 1, Replicas: 2, ReadyReplicas: 1},
	}
	ready := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "ready", Namespace: "ns", Labels: map[string]string{hpaRunLabelKey: "run1"}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
		}},
	}
	pending := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pending", Namespace: "ns", Labels: map[string]string{hpaRunLabelKey: "run1"}},
		Status: corev1.PodStatus{Phase: corev1.PodPending, Conditions: []corev1.PodCondition{
			{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable, Message: "0/1 nodes are available: 1 Insufficient nvidia.com/gpu"},
		}},
	}
	return deployment, ready, pending
}

func hpaRESTClient(t *testing.T, handler http.HandlerFunc) kubernetes.Interface {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	return client
}
