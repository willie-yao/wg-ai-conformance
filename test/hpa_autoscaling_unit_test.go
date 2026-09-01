package conformance

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
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
	values, err := parsePodMetrics([]byte(`{"items":[{"describedObject":{"kind":"Pod","name":"pod-a"},"metricName":"queue_depth","value":"100"}]}`), "queue_depth")
	if err != nil {
		t.Fatalf("parsePodMetrics unexpected error: %v", err)
	}
	if value := values["pod-a"]; value.Cmp(resource.MustParse("100")) != 0 {
		t.Fatalf("value = %s, want 100", value.String())
	}

	for _, tc := range []struct {
		name string
		data string
	}{
		{name: "invalid JSON", data: `{`},
		{name: "metric mismatch", data: `{"items":[{"describedObject":{"kind":"Pod","name":"pod-a"},"metricName":"other","value":"1"}]}`},
		{name: "missing Pod name", data: `{"items":[{"describedObject":{"kind":"Pod"},"metricName":"queue_depth","value":"1"}]}`},
		{name: "wrong kind", data: `{"items":[{"describedObject":{"kind":"Service","name":"svc"},"metricName":"queue_depth","value":"1"}]}`},
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
		diagnostic, ok := classifyAcceleratorCapacityShortfall(t.Context(), fake.NewClientset(&readyPod, &pendingPod), []corev1.Pod{readyPod, pendingPod}, allocationModeDevicePlugin, cfg)
		want := "Pod pending Unschedulable: " + message
		if !ok || diagnostic != want {
			t.Fatalf("classification = %t, diagnostic = %q, want %q", ok, diagnostic, want)
		}
	})

	t.Run("DRA capacity shortfall", func(t *testing.T) {
		claimName := "pending-claim"
		message := "0/1 nodes are available: cannot allocate all claims"
		pendingPod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pending", Namespace: "ns"}, Status: corev1.PodStatus{Phase: corev1.PodPending, Conditions: []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable, Message: message}}, ResourceClaimStatuses: []corev1.PodResourceClaimStatus{{Name: testRequestName, ResourceClaimName: &claimName}}}}
		claim := resourcev1.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Name: claimName, Namespace: "ns"}}
		diagnostic, ok := classifyAcceleratorCapacityShortfall(t.Context(), fake.NewClientset(&readyPod, &pendingPod, &claim), []corev1.Pod{readyPod, pendingPod}, allocationModeDRA, cfg)
		want := "Pod pending Unschedulable: " + message + "; generated ResourceClaim " + claimName + " is unallocated"
		if !ok || diagnostic != want {
			t.Fatalf("classification = %t, diagnostic = %q, want %q", ok, diagnostic, want)
		}
	})

	t.Run("genuine DRA driver failure", func(t *testing.T) {
		claimName := "pending-claim"
		pendingPod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pending", Namespace: "ns"}, Status: corev1.PodStatus{Phase: corev1.PodPending, Conditions: []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable, Message: "DRA driver failed to prepare the claim"}}, ResourceClaimStatuses: []corev1.PodResourceClaimStatus{{Name: testRequestName, ResourceClaimName: &claimName}}}}
		claim := resourcev1.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Name: claimName, Namespace: "ns"}}
		if diagnostic, ok := classifyAcceleratorCapacityShortfall(t.Context(), fake.NewClientset(&readyPod, &pendingPod, &claim), []corev1.Pod{readyPod, pendingPod}, allocationModeDRA, cfg); ok {
			t.Fatalf("unexpected classification: %q", diagnostic)
		}
	})

	t.Run("unrelated Unschedulable reason", func(t *testing.T) {
		pendingPod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pending", Namespace: "ns"}, Status: corev1.PodStatus{Phase: corev1.PodPending, Conditions: []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable, Message: "0/1 nodes are available: 1 node had an untolerated taint"}}}}
		if diagnostic, ok := classifyAcceleratorCapacityShortfall(t.Context(), fake.NewClientset(&readyPod, &pendingPod), []corev1.Pod{readyPod, pendingPod}, allocationModeDevicePlugin, cfg); ok {
			t.Fatalf("unexpected classification: %q", diagnostic)
		}
	})
}
