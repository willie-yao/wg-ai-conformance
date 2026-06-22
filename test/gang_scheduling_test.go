package conformance

import (
	"context"
	"flag"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Kueue API constants. The test drives Kueue, which enforces all-or-nothing
// scheduling by admitting a whole Job against a ClusterQueue's quota: a Job is
// only unsuspended (and its pods created) when the quota covers the entire Job.
// Other gang scheduling solutions (e.g. Volcano) use a different model; if one is
// needed, introduce an interface with a second implementation rather than forcing
// both into one config.
const (
	kueueAPIVersion = "kueue.x-k8s.io/v1beta2"
	kueueQueueLabel = "kueue.x-k8s.io/queue-name"
	// kueueJobUIDLabel is set by Kueue on the Workload it creates for a Job,
	// carrying the Job's UID so the Workload can be matched back to the Job.
	kueueJobUIDLabel = "kueue.x-k8s.io/job-uid"

	// jobNameLabel is set by the Job controller on the pods it creates.
	jobNameLabel = "batch.kubernetes.io/job-name"

	// The quota gates on CPU: a fitting Job stays within it, an oversized Job
	// exceeds it. Memory quota is generous so CPU is the only binding constraint.
	gangPodCPU        = "100m"
	gangPodMem        = "64Mi"
	gangQueueCPUQuota = "250m" // fits gangFitReplicas (200m), not gangOversizedReplicas (300m)
	gangQueueMemQuota = "4Gi"

	gangFitReplicas       = 2
	gangOversizedReplicas = 3
)

var (
	resourceFlavorGVR = schema.GroupVersionResource{Group: "kueue.x-k8s.io", Version: "v1beta2", Resource: "resourceflavors"}
	clusterQueueGVR   = schema.GroupVersionResource{Group: "kueue.x-k8s.io", Version: "v1beta2", Resource: "clusterqueues"}
	localQueueGVR     = schema.GroupVersionResource{Group: "kueue.x-k8s.io", Version: "v1beta2", Resource: "localqueues"}
	workloadGVR       = schema.GroupVersionResource{Group: "kueue.x-k8s.io", Version: "v1beta2", Resource: "workloads"}

	gangNegativeWait *time.Duration
)

func init() {
	gangNegativeWait = flag.Duration("gang-negative-wait", 45*time.Second, "How long to wait while confirming that no partial scheduling occurs in the negative case.")
}

// TestGangScheduling verifies the Gang Scheduling requirement: an installed gang
// scheduling solution must enforce all-or-nothing scheduling for a multi-pod
// workload, so a gang either schedules in full or not at all. It exercises Kueue,
// which gates admission on a ClusterQueue quota that the test defines.
//
// The two subtests are designed to run together: the positive case proves Kueue
// admits a gang that fits, and the negative case proves it denies one that does
// not. (Running only the negative case in isolation cannot distinguish a correct
// denial from Kueue being absent, since an unadmitted Job simply stays suspended.)
// Ref: https://github.com/kubernetes-sigs/ai-conformance/tree/main/kars/0053-gang-scheduling
func TestGangScheduling(t *testing.T) {
	if !flag.Parsed() {
		flag.Parse()
	}

	ctx := context.Background()
	config := restConfig(t)
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatalf("Error creating kubernetes client: %v", err)
	}
	dynClient, err := dynamic.NewForConfig(config)
	if err != nil {
		t.Fatalf("Error creating dynamic client: %v", err)
	}

	checkKueueInstalled(t, clientset)

	namespace := randomNamespaceName("gang-scheduling")
	createNamespace(ctx, t, clientset, namespace)
	t.Cleanup(func() { deleteNamespace(ctx, t, clientset, namespace) })

	// Define the quota the gangs are admitted against. The flavor and ClusterQueue
	// are cluster-scoped, so they are uniquely named and cleaned up explicitly.
	flavorName := randomNamespaceName("gang-flavor")
	cqName := randomNamespaceName("gang-cq")
	createResourceFlavor(ctx, t, dynClient, flavorName)
	t.Cleanup(func() { deleteClusterScoped(ctx, t, dynClient, resourceFlavorGVR, flavorName) })
	createClusterQueue(ctx, t, dynClient, cqName, flavorName)
	t.Cleanup(func() { deleteClusterScoped(ctx, t, dynClient, clusterQueueGVR, cqName) })

	queueName := "gang-queue"
	createLocalQueue(ctx, t, dynClient, namespace, queueName, cqName)

	// Positive case: a Job that fits the quota is admitted and all of its pods run.
	t.Run("AllOrNothingScheduling", func(t *testing.T) {
		jobName := "gang-fits"
		t.Cleanup(func() { deleteJob(ctx, t, clientset, namespace, jobName) })

		createGangJob(ctx, t, clientset, namespace, jobName, queueName, gangFitReplicas)
		waitJobPodsScheduled(ctx, t, clientset, namespace, jobName, gangFitReplicas, 2*time.Minute)
	})

	// Negative case: a Job that exceeds the quota is not admitted, so none of its
	// pods are ever created or scheduled (no partial scheduling).
	t.Run("NoPartialScheduling", func(t *testing.T) {
		jobName := "gang-too-big"
		t.Cleanup(func() { deleteJob(ctx, t, clientset, namespace, jobName) })

		createGangJob(ctx, t, clientset, namespace, jobName, queueName, gangOversizedReplicas)
		ensureGangNotAdmitted(ctx, t, clientset, dynClient, namespace, jobName, *gangNegativeWait)
	})
}

// restConfig builds a Kubernetes REST config from the configured kubeconfig.
func restConfig(t *testing.T) *rest.Config {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if *kubeconfig != "" {
		loadingRules.ExplicitPath = *kubeconfig
	}

	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		t.Fatalf("Error building kubeconfig: %v", err)
	}
	return config
}

// checkKueueInstalled verifies the Kueue API is served.
func checkKueueInstalled(t *testing.T, c *kubernetes.Clientset) {
	if _, err := c.Discovery().ServerResourcesForGroupVersion(kueueAPIVersion); err != nil {
		t.Fatalf("ENVIRONMENT ERROR: %s API not found. Is Kueue installed? Error: %v", kueueAPIVersion, err)
	}
}

// createResourceFlavor creates a default Kueue ResourceFlavor that matches any node.
func createResourceFlavor(ctx context.Context, t *testing.T, dyn dynamic.Interface, name string) {
	rf := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": kueueAPIVersion,
		"kind":       "ResourceFlavor",
		"metadata":   map[string]interface{}{"name": name},
	}}
	if _, err := dyn.Resource(resourceFlavorGVR).Create(ctx, rf, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create ResourceFlavor %s: %v", name, err)
	}
}

// createClusterQueue creates a ClusterQueue with a small CPU quota that gates
// gang admission. The empty namespaceSelector lets any namespace use it.
func createClusterQueue(ctx context.Context, t *testing.T, dyn dynamic.Interface, name, flavor string) {
	cq := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": kueueAPIVersion,
		"kind":       "ClusterQueue",
		"metadata":   map[string]interface{}{"name": name},
		"spec": map[string]interface{}{
			"namespaceSelector": map[string]interface{}{},
			"resourceGroups": []interface{}{
				map[string]interface{}{
					"coveredResources": []interface{}{"cpu", "memory"},
					"flavors": []interface{}{
						map[string]interface{}{
							"name": flavor,
							"resources": []interface{}{
								map[string]interface{}{"name": "cpu", "nominalQuota": gangQueueCPUQuota},
								map[string]interface{}{"name": "memory", "nominalQuota": gangQueueMemQuota},
							},
						},
					},
				},
			},
		},
	}}
	if _, err := dyn.Resource(clusterQueueGVR).Create(ctx, cq, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create ClusterQueue %s: %v", name, err)
	}
}

// createLocalQueue creates a namespaced LocalQueue pointing at the ClusterQueue.
func createLocalQueue(ctx context.Context, t *testing.T, dyn dynamic.Interface, ns, name, clusterQueue string) {
	lq := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": kueueAPIVersion,
		"kind":       "LocalQueue",
		"metadata":   map[string]interface{}{"name": name, "namespace": ns},
		"spec":       map[string]interface{}{"clusterQueue": clusterQueue},
	}}
	if _, err := dyn.Resource(localQueueGVR).Namespace(ns).Create(ctx, lq, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create LocalQueue %s: %v", name, err)
	}
}

// createGangJob creates a suspended Job of the given size, labeled for the Kueue
// queue. Kueue admits (unsuspends) it only when the queue quota covers the whole Job.
func createGangJob(ctx context.Context, t *testing.T, c *kubernetes.Clientset, ns, name, queue string, replicas int32) {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels:    map[string]string{kueueQueueLabel: queue},
		},
		Spec: batchv1.JobSpec{
			Suspend:     ptrTo(true),
			Parallelism: ptrTo(replicas),
			Completions: ptrTo(replicas),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:    "worker",
						Image:   "ubuntu:22.04",
						Command: []string{"sleep", "3600"},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse(gangPodCPU),
								corev1.ResourceMemory: resource.MustParse(gangPodMem),
							},
						},
					}},
				},
			},
		},
	}
	if _, err := c.BatchV1().Jobs(ns).Create(ctx, job, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create gang Job %s: %v", name, err)
	}
}

// waitJobPodsScheduled fails the test unless all of the Job's pods are bound to a
// node within the timeout, confirming Kueue admitted the gang and it scheduled
// together. Binding (rather than Running) is the precise placement signal.
func waitJobPodsScheduled(ctx context.Context, t *testing.T, c *kubernetes.Clientset, ns, jobName string, want int, timeout time.Duration) {
	t.Logf("Waiting for all %d pods of Job %s to be scheduled...", want, jobName)
	err := wait.PollUntilContextTimeout(ctx, 3*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		pods, err := listJobPods(ctx, c, ns, jobName)
		if err != nil || len(pods) < want {
			return false, nil
		}
		scheduled := 0
		for i := range pods {
			if pods[i].Spec.NodeName != "" {
				scheduled++
			}
		}
		return scheduled >= want, nil
	})

	if err != nil {
		t.Errorf("FAIL: Job %s did not have %d pods scheduled within %s. Is Kueue admitting the workload?", jobName, want, timeout)
		logJobStatus(ctx, t, c, ns, jobName)
	} else {
		t.Logf("PASS: all %d pods of Job %s were admitted and scheduled together.", want, jobName)
	}
}

// ensureGangNotAdmitted verifies the oversized gang is actively denied by Kueue.
// It first confirms Kueue created a Workload for the Job (proving Kueue's Job
// integration reconciled it, so a failure to schedule reflects a real quota
// denial rather than Kueue being absent or inert), then asserts that throughout
// the window the Workload stays unadmitted, the Job stays suspended, and no pod
// is scheduled.
func ensureGangNotAdmitted(ctx context.Context, t *testing.T, c *kubernetes.Clientset, dyn dynamic.Interface, ns, jobName string, window time.Duration) {
	jobUID := getJobUID(ctx, t, c, ns, jobName)

	// Kueue creates the Workload shortly after the Job; require it to appear so a
	// "nothing scheduled" result cannot pass when Kueue never evaluated the gang.
	t.Logf("Waiting for Kueue to create a Workload for Job %s...", jobName)
	if err := wait.PollUntilContextTimeout(ctx, 2*time.Second, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		wl, err := jobWorkload(ctx, dyn, ns, jobUID)
		return err == nil && wl != nil, nil
	}); err != nil {
		t.Errorf("FAIL: Kueue did not create a Workload for Job %s; cannot confirm the gang was actively denied. Is the Kueue Job integration enabled?", jobName)
		return
	}

	t.Logf("Confirming the oversized gang stays denied (Workload unadmitted, Job suspended, no pods scheduled) for %s...", window)
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		if wl, err := jobWorkload(ctx, dyn, ns, jobUID); err == nil && wl != nil && workloadAdmitted(wl) {
			t.Errorf("VIOLATION: Kueue admitted Workload %s for Job %s despite it exceeding the queue quota.", wl.GetName(), jobName)
			logJobStatus(ctx, t, c, ns, jobName)
			return
		}
		if job, err := c.BatchV1().Jobs(ns).Get(ctx, jobName, metav1.GetOptions{}); err == nil {
			if job.Spec.Suspend == nil || !*job.Spec.Suspend {
				t.Errorf("VIOLATION: Job %s was unsuspended (admitted by Kueue) despite exceeding the queue quota.", jobName)
				logJobStatus(ctx, t, c, ns, jobName)
				return
			}
		}
		if pods, err := listJobPods(ctx, c, ns, jobName); err == nil {
			for i := range pods {
				if pods[i].Spec.NodeName != "" {
					t.Errorf("VIOLATION: pod %s of Job %s was scheduled to node %s; the oversized gang must not be partially scheduled.", pods[i].Name, jobName, pods[i].Spec.NodeName)
					logJobStatus(ctx, t, c, ns, jobName)
					return
				}
			}
		}
		time.Sleep(3 * time.Second)
	}
	t.Logf("PASS: Kueue left the oversized gang's Workload unadmitted; no partial scheduling.")
}

// getJobUID returns the UID of the named Job, used to find its Kueue Workload.
func getJobUID(ctx context.Context, t *testing.T, c *kubernetes.Clientset, ns, name string) string {
	job, err := c.BatchV1().Jobs(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get Job %s: %v", name, err)
	}
	return string(job.UID)
}

// jobWorkload returns the Kueue Workload created for the Job (matched by the
// job-uid label), or nil if Kueue has not created it yet.
func jobWorkload(ctx context.Context, dyn dynamic.Interface, ns, jobUID string) (*unstructured.Unstructured, error) {
	list, err := dyn.Resource(workloadGVR).Namespace(ns).List(ctx, metav1.ListOptions{LabelSelector: kueueJobUIDLabel + "=" + jobUID})
	if err != nil {
		return nil, err
	}
	if len(list.Items) == 0 {
		return nil, nil
	}
	return &list.Items[0], nil
}

// workloadAdmitted reports whether Kueue has admitted the Workload, i.e. reserved
// quota for it (status.admission is set).
func workloadAdmitted(wl *unstructured.Unstructured) bool {
	_, found, err := unstructured.NestedMap(wl.Object, "status", "admission")
	return err == nil && found
}

// listJobPods returns the pods created by the named Job.
func listJobPods(ctx context.Context, c *kubernetes.Clientset, ns, jobName string) ([]corev1.Pod, error) {
	pods, err := c.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: jobNameLabel + "=" + jobName})
	if err != nil {
		return nil, err
	}
	return pods.Items, nil
}

// deleteJob deletes the Job (cascading to its pods) and waits for the Job to be
// gone, so its Kueue Workload and the quota it held are released before the next
// scenario runs.
func deleteJob(ctx context.Context, t *testing.T, c *kubernetes.Clientset, ns, name string) {
	policy := metav1.DeletePropagationBackground
	if err := c.BatchV1().Jobs(ns).Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: &policy}); err != nil && !apierrors.IsNotFound(err) {
		t.Errorf("Failed to delete Job %s: %v", name, err)
		return
	}

	wait.PollUntilContextTimeout(ctx, 2*time.Second, 1*time.Minute, true, func(ctx context.Context) (bool, error) {
		if _, err := c.BatchV1().Jobs(ns).Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			return false, nil
		}
		pods, err := listJobPods(ctx, c, ns, name)
		return err == nil && len(pods) == 0, nil
	})
}

// deleteClusterScoped deletes a cluster-scoped Kueue object, tolerating absence.
func deleteClusterScoped(ctx context.Context, t *testing.T, dyn dynamic.Interface, gvr schema.GroupVersionResource, name string) {
	if err := dyn.Resource(gvr).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		t.Errorf("Failed to delete %s %s: %v", gvr.Resource, name, err)
	}
}

// createNamespace creates the test namespace, tolerating a pre-existing one.
func createNamespace(ctx context.Context, t *testing.T, c *kubernetes.Clientset, ns string) {
	if _, err := c.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("Failed to create namespace: %v", err)
	}
}

// deleteNamespace deletes the test namespace and waits for it to be gone so that
// resources are released for subsequent runs.
func deleteNamespace(ctx context.Context, t *testing.T, c *kubernetes.Clientset, ns string) {
	t.Logf("Cleaning up namespace %s...", ns)
	if err := c.CoreV1().Namespaces().Delete(ctx, ns, metav1.DeleteOptions{}); err != nil {
		t.Errorf("Failed to cleanup namespace: %v", err)
		return
	}

	err := wait.PollUntilContextTimeout(ctx, 2*time.Second, 1*time.Minute, true, func(ctx context.Context) (bool, error) {
		_, err := c.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
		return apierrors.IsNotFound(err), nil
	})
	if err != nil {
		t.Errorf("CLEANUP FAILURE: Failed to delete namespace %s: %v. "+
			"Please ensure this namespace is terminated manually to avoid resource leaks.", ns, err)
	}
}

// logJobStatus logs the Job's suspend state and its pods to aid debugging.
func logJobStatus(ctx context.Context, t *testing.T, c *kubernetes.Clientset, ns, jobName string) {
	if job, err := c.BatchV1().Jobs(ns).Get(ctx, jobName, metav1.GetOptions{}); err == nil {
		suspended := job.Spec.Suspend != nil && *job.Spec.Suspend
		t.Logf("  job %s: suspended=%v active=%d", jobName, suspended, job.Status.Active)
	}
	pods, err := listJobPods(ctx, c, ns, jobName)
	if err != nil {
		t.Logf("  <failed to list pods for job %s: %v>", jobName, err)
		return
	}
	for _, p := range pods {
		t.Logf("  pod %s: phase=%s node=%q", p.Name, p.Status.Phase, p.Spec.NodeName)
	}
}

// ptrTo returns a pointer to v.
func ptrTo[T any](v T) *T { return &v }
