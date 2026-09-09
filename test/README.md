## Prerequisites
To run these AI Conformance tests, you must have:

- Golang: Installed on your local machine.
- Kubeconfig: A valid kubeconfig file with cluster-admin permissions for the target cluster.
- Accelerator Node Pool: The cluster must have nodes with accelerators exposed through the Kubernetes resource management framework — either a DRA driver (ResourceClaims against a DeviceClass such as `gpu.nvidia.com`) or a device plugin (extended resources such as `nvidia.com/gpu`). Make sure your nodes allow testing pods to be scheduled on them (e.g. no taints that prevent scheduling).
- Cluster Autoscaling Test: `TestAcceleratorClusterAutoscaling` additionally requires a running cluster autoscaler and an isolated accelerator pool with minimum size `N >= 1`, maximum size at least `N+1`, effective capacity for exactly one requested accelerator per baseline node, scale-down enabled, sufficient cloud quota/stock, and one stable node label inherited by new pool nodes. The pool must contain no non-DaemonSet workloads or other Pending Pods explicitly selecting the pool. Device-plugin mode permits unrelated running accelerator workloads outside the pool but rejects other Pending Pods requesting the configured extended resource. DRA mode requires no other active Pods with ResourceClaims or allocated ResourceClaims outside the test namespace while the test runs because DRA devices may use shared topology.
- HPA Autoscaling Test: `TestAcceleratorHorizontalPodAutoscaling` additionally requires the `autoscaling/v2` HPA API, a preconfigured `custom.metrics.k8s.io` adapter, API server Pod proxy connectivity, and capacity for two simultaneous accelerator allocations.
- Network Access: The test machine must be able to reach the Kubernetes API server.

## Running the Tests

Run the tests using the standard go test command. You should run tests with the same release tag as your cluster version, to ensure compatibility with the Kubernetes AI conformance program.

By default, the test looks for your kubeconfig at `~/.kube/config`. You can override this by setting the `KUBECONFIG` environment variable or using the `-kubeconfig` flag.

```bash
export KUBECONFIG=/path/to/my/config # Optional
go test -v ./test [-run <TestName>] [flags...]
```

Run `go test ./test -args -help` for details about each flag.

The allocation-mode detection, pod-construction, and device-probe logic also has hermetic unit tests that need no cluster (Kubernetes API tests use a fake clientset):

```bash
go test -v -short ./test
```

### Test Cases Covered

| Test Name | Requirement Covered | Requirement Level |
|-|-|-|
| `TestSecureAcceleratorAccess` | Secure Accelerator Access | MUST |
| `TestGangScheduling` | Gang Scheduling | MUST |
| `TestAcceleratorClusterAutoscaling` | Effective Cluster Autoscaling for Accelerators | MUST |
| `TestAcceleratorHorizontalPodAutoscaling` | Effective HPA Autoscaling for AI Workloads | MUST |

### Accelerator Cluster Autoscaling

The autoscaling test observes a preconfigured accelerator pool. The test requires an explicit `-autoscaler-node-pool-label` flag and is **skipped by default** if the flag is unset. 

If your platform provides cluster autoscaling, you must set this flag and run the test. If cluster autoscaling is not supported (N/A), you can leave the flag unset to skip the test.

Scale-up and scale-down can take significantly longer than Go's default test timeout, so it is recommended to use `-timeout 75m` or a larger value. The test also includes configurable observation windows (`-autoscaler-scale-up-timeout`, `-autoscaler-scale-down-timeout`, etc.) since node provisioning times vary heavily by cloud provider. 

Run `go test ./test -args -help` for details on all supported flags.

### Accelerator Horizontal Pod Autoscaling

`TestAcceleratorHorizontalPodAutoscaling` verifies that an `autoscaling/v2`
HorizontalPodAutoscaler can scale an accelerator-backed Deployment from one to
two Ready replicas and back to one using a per-Pod custom metric. It supports
both DRA and device-plugin allocation through the suite-wide `-allocation-mode`
flag. Two simultaneous accelerator allocations are required independently of
whether the platform provides cluster autoscaling.

The test does not install or modify an adapter, APIService, RBAC, or monitoring
resource. Before running it, the platform must serve
`custom.metrics.k8s.io/v1beta1`, allow API server Pod proxy requests, and expose
the configured resource-consumer metric unchanged as an instantaneous per-Pod
gauge. Verify the APIService and discovery endpoint:

```bash
kubectl get apiservice v1beta1.custom.metrics.k8s.io
kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1"
```

Use the same metric name for the workload and the adapter's API. With default
prometheus-adapter rules, avoid names ending in `_total` or `_seconds_total`,
which are wrapped in `rate()`, and names beginning with `container_`, which are
treated as cAdvisor metrics. These names can work with custom rules that expose
the unchanged gauge; the test requires the high value to equal exactly 100.

The test skips before kubeconfig access in short mode. Outside short mode it
skips only when `-hpa-custom-metric-name` is empty. A skipped result is not
conformance evidence. Use manual attestation when HPA is supported only through
external, object, or push-based metrics. Mark `pod_autoscaling` `N/A` only when
HPA itself is unsupported.

The three HPA-specific flags are:

- `-hpa-custom-metric-name=<metric>` opts in and names the per-Pod gauge.
- `-hpa-namespace=<namespace>` uses an existing namespace; otherwise the test
  creates and deletes a namespace.
- `-hpa-metric-timeout=<duration>` bounds each metric propagation wait and
  defaults to `12m`, allowing a common 10-minute adapter relist interval plus
  scrape and propagation time.

Created Pods have `prometheus.io/scrape`, `prometheus.io/port`, and
`prometheus.io/path` annotations. Label-based monitoring must select
`ai-conformance.kubernetes.io/test=kar-0057`. The generated-namespace default
requires cluster-wide scrape coverage. Otherwise, use `-hpa-namespace` with a
namespace that the operator's monitoring already watches. A separate run label
isolates the test's Pod and metric queries.

The resource-consumer `/BumpMetric` endpoint raises the initial Pod's metric to
100 before HPA creation. Each bump is single-shot and expires after seven days,
so it cannot revert during the normal test run. After scale-up, the test
registers a zero-valued series for the second Pod and observes both series.
It then lowers the initial Pod's metric and requires both series to read zero.

The HPA starts with downscaling disabled so it cannot remove a Pod before both
zeros are observed. The test then enables the normal `Max` downscale policy,
retaining a 60-second stabilization window, and requires one desired and Ready
replica for 30 seconds. It never changes replica counts directly.

Initial readiness has a 10-minute limit. Scale-up has two serial 10-minute
waits: HPA desired replicas, then Deployment readiness. Final stable scale-down
has another 10-minute limit. Each of the three metric waits has its own
`-hpa-metric-timeout` allowance.

If the second-replica readiness wait fails, the test makes a fresh, bounded
observation of the run's Pods. A narrowly identified accelerator-capacity
scheduling shortfall produces `ENVIRONMENT ERROR:` alongside the original
readiness error. DRA attribution additionally requires an unallocated generated
claim. This is best-effort scheduling evidence, not proof of the sole cause;
failed diagnostic reads are reported without positive capacity attribution.
Initial-replica readiness failures remain accelerator workload failures.
Metrics and HPA failures retain their respective failure categories. All these
outcomes fail the Go test; none is a skip or a separate result status.

Cleanup requests foreground Deployment deletion and waits for the Deployment
and run's Pods to disappear before completing DRA allocation-release checks.
It deletes the HPA and run-scoped claim template, reports incomplete cleanup,
and leaves a supplied namespace and monitoring configuration unchanged.

Use `-timeout 90m` with default flags. The serial polling, bounded requests, and
cleanup allowances total up to 84 minutes with a generated namespace or 82
minutes with a supplied namespace. Increase the harness timeout if increasing
`-hpa-metric-timeout`, which is used three times. This is not a hard end-to-end
guarantee: existing setup and allocation-verification API calls follow the
suite's unbounded request policy, so a stalled call can still cause the Go
harness to abort before cleanup.

```bash
go test -v ./test \
  -run '^TestAcceleratorHorizontalPodAutoscaling$' \
  -timeout 90m \
  -accelerator-type=nvidia \
  -allocation-mode=device-plugin \
  -hpa-custom-metric-name=ai_conformance_queue_depth \
  -hpa-namespace=ai-conformance-hpa
```

Repeat with `-allocation-mode=dra` to validate DRA. HPA-specific hermetic coverage
includes `TestBuildHPADeployment`, `TestBuildAcceleratorHPA`,
`TestParsePodMetrics`, `TestEnableHPAScaleDown`, `TestCleanupHPAWorkload`,
`TestClassifyAcceleratorCapacityShortfall`, `TestWaitForHPAScaleUp`,
`TestWaitForDeploymentReplicas`, `TestWaitForHPAReplicas`, and
`TestWaitForPodMetrics`.

## Vendor Customization & Neutrality

The tests are designed to be vendor-neutral where possible, but hardware-level probing often requires vendor-specific configuration. If your platform uses different hardware/software not covered by the tests, please file an issue to request support for your hardware/software. In the meantime, you will need to certify manually.

### Opting Out (N/A Requirements)

If a specific requirement is not applicable to your platform (i.e., you answered "N/A" in the conformance checklist), you may skip or "opt-out" of that specific sub-test. To skip tests during execution, you can use test-specific flags (if available) or use the `go test -run <regex>` flag to select only the applicable tests. When submitting your results, ensure that you provide a clear explanation for why the requirement is considered N/A.

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

Starting with v1.37, it is **recommended** that any requirement covered by an automated test is verified using this test suite. When submitting your conformance results, you can use the generated `e2e.log` and `results.json` (or `junit.xml`) as evidence for these automated test cases. Link them in your `KubernetesAIConformance-1.NN.yaml` file under the respective requirement's `evidence` field.

