# KAR-0057: Effective HPA Autoscaling for AI Workloads

## Description

If the platform supports the HorizontalPodAutoscaler, it must function correctly for pods utilizing accelerators. This includes the ability to scale these Pods based on custom metrics relevant to AI/ML workloads.

## Motivation

AI workloads often need to scale horizontally based on demand. For example, GPU-backed inference services may need to scale up when request latency increases or queue depth grows. The HorizontalPodAutoscaler (HPA) is the standard Kubernetes mechanism for this, but it must work correctly with pods that consume accelerator resources and scale based on custom metrics (e.g. accelerator utilization, request throughput) rather than just CPU and memory.

Without HPA support for accelerator-backed pods and custom metrics, operators must manually scale these workloads or build custom autoscaling solutions, which adds complexity and reduces reliability.

## Graduation Criteria

**SHOULD**
- [x] Describe how users can test it for self-attestation with scripts, documentation, etc
- [ ] Starting v1.37, new SHOULDs must include proposed automated tests in the automated tests section below

**MUST**
- [ ] Starting v1.37, new MUSTs must include automated tests that have been added to the AI conformance test suite
- [x] Demonstrate at least two real-world usage of SHOULD before graduating to MUST
- [x] Kubernetes core APIs must be GA

## Test Plan

### How We Might Test It

A custom metrics pipeline is configured to expose accelerator-related custom metrics to the HPA. Create a Deployment with each Pod requesting an accelerator and exposing a custom metric. Create a HorizontalPodAutoscaler targeting the Deployment. Introduce load to the sample application, causing the average custom metric value to significantly exceed the target, triggering a scale up. Then remove the load to trigger a scale down.

### Automated Tests

The automated test uses a preconfigured custom metrics pipeline and does not install, replace, or remove an adapter or monitoring resource.

1. **Applicability**: Outside short mode, run the test with a per-Pod gauge name when HPA is supported through `custom.metrics.k8s.io/v1beta1`; otherwise use the requirement's manual attestation path. A short-mode or unconfigured skip is not conformance evidence.
2. **Metric preflight**: Create one accelerator-backed replica, raise its metric to exactly 100, and verify the unchanged value through the custom metrics API before creating the HPA.
3. **Scale up**: Create an `autoscaling/v2` HPA with a `Pods` metric target and downscaling disabled. Verify that it requests two replicas and both accelerator-backed Pods become Ready. If readiness fails, fresh evidence of an accelerator-capacity scheduling shortfall produces `ENVIRONMENT ERROR:` with the Unschedulable condition and original readiness error.
4. **Scale down**: Register the second Pod's zero series, lower the first Pod to zero, and verify both series are zero before enabling HPA downscaling. Require a stable return to one Ready replica without directly changing replica counts.
5. **Cleanup**: Delete test resources, wait for the Deployment and Pods to disappear and generated claims to be removed or deallocated, and leave a supplied namespace and its monitoring configuration unchanged. Incomplete cleanup fails the test.

This is implemented by [`TestAcceleratorHorizontalPodAutoscaling`](../../test/hpa_autoscaling_test.go).

## Implementation History

2026-03-12: KAR created

## Related KARs

<!--
List KARS that are related. This is in case of additional requirements that come up after a KAR has already graduated to "implemented"
-->
