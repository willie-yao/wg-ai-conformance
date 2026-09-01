## Prerequisites
To run these AI Conformance tests, you must have:

- Golang: Installed on your local machine.
- Kubeconfig: A valid kubeconfig file with cluster-admin permissions for the target cluster.
- Accelerator Node Pool: The cluster must have nodes with accelerators exposed through the Kubernetes resource management framework, either a DRA driver (ResourceClaims against a DeviceClass such as `gpu.nvidia.com`) or a device plugin (extended resources such as `nvidia.com/gpu`). Make sure your nodes allow testing pods to be scheduled on them (e.g. no taints that prevent scheduling).
- Cluster Autoscaling Test: `TestAcceleratorClusterAutoscaling` additionally requires a running cluster autoscaler and an isolated accelerator pool with minimum size `N >= 1`, maximum size at least `N+1`, effective capacity for exactly one requested accelerator per baseline node, scale-down enabled, sufficient cloud quota/stock, and one stable node label inherited by new pool nodes. The pool must contain no non-DaemonSet workloads or other Pending Pods explicitly selecting the pool. Device-plugin mode permits unrelated running accelerator workloads outside the pool but rejects other Pending Pods requesting the configured extended resource. DRA mode requires no other active Pods with ResourceClaims or allocated ResourceClaims outside the test namespace while the test runs because DRA devices may use shared topology.
- HPA Autoscaling Test: `TestAcceleratorHorizontalPodAutoscaling` additionally requires the `autoscaling/v2` HPA API, a preconfigured `custom.metrics.k8s.io` adapter, API server Pod proxy connectivity, and capacity for two simultaneous accelerator allocations.
- Network Access: The test machine must be able to reach the Kubernetes API server.

## Running the Tests

Run the tests using the standard go test command. You should run tests with the same release tag as your cluster version, to ensure compatibility with the Kubernetes AI conformance program.

By default, the test looks for your kubeconfig at `~/.kube/config`. You can override this by setting the `KUBECONFIG` environment variable or using the `-kubeconfig` flag.

```bash
export KUBECONFIG=/path/to/my/config # Optional
go test -v ./test [-run <TestName>] [-kubeconfig=<path/to/kubeconfig>] [-accelerator-type=<type>] [-allocation-mode=<mode>]
```

Run `go test ./test -args -help` for details about each flag.

The allocation-mode detection, pod-construction, and device-probe logic also has hermetic unit tests that need no cluster (Kubernetes API tests use a fake clientset):

```bash
go test -v ./test -run 'Test(DetectAllocationMode|ExtendedResourceGuardFailsClosed|LookupAcceleratorConfig|BuildTestPod|PodGeneratedClaims|DeleteAndAwaitRelease|AcceleratorProbeCommand|LogsContainExactLine|BuildHPADeployment|ParsePodMetrics|ClassifyAcceleratorCapacityShortfall)$'
```

### Test Cases Covered

| Test Name | Requirement Covered | Requirement Level |
|-|-|-|
| `TestSecureAcceleratorAccess` | Secure Accelerator Access | MUST |
| `TestGangScheduling` | Gang Scheduling | MUST |
| `TestAcceleratorClusterAutoscaling` | Effective Cluster Autoscaling for Accelerators | MUST |
| `TestAcceleratorHorizontalPodAutoscaling` | Effective HPA Autoscaling for AI Workloads | MUST |

### Accelerator Cluster Autoscaling

The autoscaling test observes a preconfigured accelerator pool; it does not
modify provider node-pool settings. Use a node label that uniquely identifies
the target pool. The example below uses the AKS `agentpool` label; replace it
with the corresponding label for your platform.

The test is skipped when `-autoscaler-node-pool-label` is unset. Platforms that
do not provide a cluster autoscaler or equivalent mechanism may mark the
`cluster_autoscaling` requirement as `N/A` with a justification and leave the
flag unset. Platforms that provide autoscaling must configure the flag and run
the test; a skipped result is not evidence that the requirement is implemented.

Baseline Pods use hostname anti-affinity to occupy distinct pool nodes. The
trigger Pod carries neither that anti-affinity nor its matching label, so
exhausted accelerator capacity is the only target-pool constraint that can
leave it Pending. A PodDisruptionBudget protects the baseline Pods while the
added node is idle and eligible for scale-down. The test fails if pool
membership changes or foreign accelerator demand appears during scale-up.

Device-plugin mode rejects extended resources backed by a DRA DeviceClass and
checks every observed pool node for exactly one allocatable extended resource.
DRA has no portable per-node allocatable count, so the isolated baseline Pods
and Pending trigger verify that the selected DeviceClass has effective capacity
for exactly `N` allocations before scale-up.

The suite-wide `-allocation-mode` flag controls the allocation mechanism for
all accelerator tests. Its default `auto` mode prefers DRA when usable and
otherwise falls back to the device plugin. Use an explicit mode when testing a
hybrid cluster where the selected pool supports only one mechanism.

Scale-up and scale-down can take significantly longer than Go's default test
timeout, so use `-timeout 75m` or a larger value.

```bash
go test -v ./test \
  -run TestAcceleratorClusterAutoscaling \
  -timeout 75m \
  -accelerator-type=nvidia \
  -allocation-mode=device-plugin \
  -autoscaler-node-pool-label=agentpool=gpupool
```

To test DRA autoscaling, the cluster autoscaler must have DRA support enabled:

```bash
go test -v ./test \
  -run TestAcceleratorClusterAutoscaling \
  -timeout 75m \
  -accelerator-type=nvidia \
  -allocation-mode=dra \
  -autoscaler-node-pool-label=agentpool=gpupool
```

The following optional flags control observation windows:

- `-autoscaler-pending-timeout` (default `2m`)
- `-autoscaler-scale-up-timeout` (default `20m`)
- `-autoscaler-scale-down-timeout` (default `30m`)
- `-autoscaler-stability-window` (default `30s`)

Scale-down passes when the original baseline Node UIDs remain Ready and schedulable, the added
Node UID is no longer Ready or has been deleted, and the selected pool has
exactly `N` Ready Nodes for the stability window. The Kubernetes API cannot
portably prove a cloud provider's node-group size, and Cluster Autoscaler does
not itself guarantee deletion of the Kubernetes `Node` object.

### Accelerator Horizontal Pod Autoscaling

`TestAcceleratorHorizontalPodAutoscaling` verifies that an `autoscaling/v2`
HorizontalPodAutoscaler can scale an accelerator-backed Deployment from one to
two Ready replicas and back to one using a per-Pod custom metric. It supports
both DRA and device-plugin allocation through the suite-wide `-allocation-mode`
flag and requires capacity for two simultaneous accelerator allocations.

The Go test does not install or modify a custom metrics adapter, APIService,
RBAC, PodMonitor, or PodMonitoring resource. Before the test runs, the platform
must serve `custom.metrics.k8s.io/v1beta1`, allow API server Pod proxy requests,
and expose the configured resource-consumer metric unchanged as an
instantaneous per-Pod gauge. The name exposed by the adapter must exactly match
`-hpa-custom-metric-name`.

Before running the test, verify that the custom metrics APIService and discovery
endpoint are available:

```bash
kubectl get apiservice v1beta1.custom.metrics.k8s.io
kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1"
```

The value passed to `-hpa-custom-metric-name` must not end in `_total` or
`_seconds_total` and must not begin with `container_`. The default
prometheus-adapter rules wrap `_total` series in `rate()` and treat `container_`
series as cAdvisor container metrics, while the test requires the value to equal
exactly 100. Use the same metric name for the workload bump and the series served
by the adapter.

The test is skipped only when `-hpa-custom-metric-name` is unset, before
kubeconfig access. A skipped result is not conformance evidence. Use manual
attestation when HPA is supported only through external, object, or push-based
metrics. Mark `pod_autoscaling` `N/A` only when HPA itself is unsupported.

The three HPA-specific flags are:

- `-hpa-custom-metric-name=<metric>` opts in and names the per-Pod gauge.
- `-hpa-namespace=<namespace>` uses an existing namespace. When omitted, the
  test creates and deletes a namespace.
- `-hpa-metric-timeout=<duration>` controls metric propagation waits and
  defaults to `12m`. This accommodates a common 10-minute adapter relist
  interval plus scrape and propagation time.

The generated-namespace default requires cluster-wide scrape coverage. If an
operator's PodMonitor or ServiceMonitor watches only selected namespaces, pass
`-hpa-namespace` with a namespace that configuration already watches.

Scale-up and scale-down each use a fixed 10-minute timeout. The HPA has a fixed
60-second scale-down stabilization window, and the final one-replica state must
remain stable for 30 seconds.

The resource-consumer `/BumpMetric` endpoint raises the initial Pod's metric to
exactly 100 before HPA creation. Every bump is single-shot and expires after
seven days, so it cannot silently revert during the test. After scale-up, a
zero-valued series is registered for the second Pod. This is not needed for HPA
calculation. It makes the final all-zero assertion non-vacuous by proving that
both scaled Pods were represented by the custom metrics API.

Created Pods carry the standard `prometheus.io/scrape`, `prometheus.io/port`,
and `prometheus.io/path` annotations. Label-based monitoring must select the
fixed `ai-conformance.kubernetes.io/test=kar-0057` label in the supplied
namespace. The run label is used internally to isolate Pod and metric queries.

If the HPA requests two replicas but the second Pod has a narrowly identified
accelerator-capacity scheduling failure, the test fails with `ENVIRONMENT
ERROR:` and preserves the scheduler condition. Other scheduling, image,
readiness, claim, metrics, or HPA failures remain test failures. Cleanup removes
the HPA, Deployment, Pods, generated claims, and run-scoped claim template. A
supplied namespace and its monitoring configuration are left unchanged.

A healthy run finishes in a few minutes, but use `-timeout 90m` so the harness
exceeds the test's internal polling and cleanup budgets; otherwise the Go test
binary can panic before the test emits its own `ENVIRONMENT ERROR:`, `METRICS
PIPELINE FAILURE:`, or accelerator-capacity diagnostic.

```bash
go test -v ./test \
  -run '^TestAcceleratorHorizontalPodAutoscaling$' \
  -timeout 90m \
  -accelerator-type=nvidia \
  -allocation-mode=device-plugin \
  -hpa-custom-metric-name=ai_conformance_queue_depth \
  -hpa-namespace=ai-conformance-hpa
```

Repeat with `-allocation-mode=dra` to validate DRA. The HPA-specific hermetic
coverage consists of `TestBuildHPADeployment`, `TestParsePodMetrics`, and
`TestClassifyAcceleratorCapacityShortfall`.

## Vendor Customization & Neutrality

The tests are designed to be vendor-neutral where possible, but hardware-level probing often requires vendor-specific configuration. If your platform uses different hardware/software not covered by the tests, please file an issue to request support for your hardware/software. In the meantime, you will need to certify manually.

### Opting Out

If a specific test is not applicable to your platform (i.e., you answered "N/A" to the corresponding question in the questionnaire), you may "opt-out" of that specific sub-test.

## Automation, CI & Certification Submissions

For CI environments and certification submissions, you can output the results in both human-readable and machine-readable formats. Use the following commands to generate the required artifacts for your `KubernetesAIConformance-1.NN.yaml` evidence:

### Generating Test Artifacts

```bash
# 1. Capture full output log (e2e.log)
go test -v ./test | tee e2e.log

# 2. Generate machine-readable JSON (results.json)
go test -v ./test -json > results.json

# 3. Generate JUnit XML report (junit.xml)
# Option A: Using gotestsum (Recommended - runs via go run)
go run gotest.tools/gotestsum@latest --junitfile junit.xml -- ./test
# Option B: Using go-junit-report
go test -v ./test 2>&1 | go run github.com/jstemmer/go-junit-report/v2@latest > junit.xml
```

### Using Test Results for Certification (Hybrid Approach)

When submitting your conformance results for v1.37+, you can use the generated `e2e.log` and `results.json` (or `junit.xml`) as evidence for automated test cases. Link them in your `KubernetesAIConformance-1.NN.yaml` file under the respective requirement's `evidence` field.


