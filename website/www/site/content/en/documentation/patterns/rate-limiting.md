---
title: "Rate limiting patterns"
---
<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Rate limiting patterns

Apache Beam is built to maximize throughput by scaling workloads across thousands of workers. However, this massive parallelism requires coordination when pipelines interact with external systems that enforce strict quotas, such as 3rd-party REST APIs, databases, or internal microservices. Without a centralized rate limiting mechanism, independent workers might exceed the capacity of these systems, resulting in service degradation or broad IP blocking.

## Centralized Rate Limit Service

The recommended approach for global rate limiting in Beam is using a centralized Rate Limit Service (RLS).

A production-ready Terraform module to deploy this service on GKE is available in the beam repository:
[`envoy-ratelimiter`](https://github.com/apache/beam/tree/master/examples/terraform/envoy-ratelimiter)

To deploy the rate-limiting infrastructure on GKE:

1. Update `terraform.tfvars` with your project variables to adjust rules and domains.
2. Run the helper deploy script: `./deploy.sh`

This script automates deployment and, upon completion, returns the Internal Load Balancer IP address for your deployment that you will use in your pipeline.

---

{{< language-switcher java py >}}

## Using RateLimiter

To rate limit requests in your pipeline, you can create a RateLimiter client in your `DoFn`'s setup phase and acquire permits before making calls in the process phase.

{{< paragraph class="language-java" >}}
In Java, use the `RateLimiter` interface and `EnvoyRateLimiterFactory` implementation to coordinate with the Envoy service. Create `RateLimiterOptions` with your service address, initialize the client in @Setup using `EnvoyRateLimiterFactory`, and call `rateLimiter.allow(batchSize)` in @ProcessElement to acquire a batch of permits.
{{< /paragraph >}}

{{< highlight java >}}
{{< code_sample "examples/java/src/main/java/org/apache/beam/examples/RateLimiterSimple.java" RateLimiterSimpleJava >}}
{{< /highlight >}}

{{< paragraph class="language-py" >}}
In Python, use the `EnvoyRateLimiter` and <a href="/documentation/patterns/shared-class/" style="text-decoration: underline;">Shared</a> to coordinate a single client instance shared across threads. Initialize client in `setup()` using `shared`, and call `self.rate_limiter.allow()` in `process()` to acquire rate-limiting permits before executing API calls.
{{< /paragraph >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/rate_limiter_simple.py" RateLimiterSimplePython >}}
{{< /highlight >}}

{{< paragraph class="language-py" >}}
If you are using **RunInference** for remote model inference (e.g., Vertex AI), you can pass the `EnvoyRateLimiter` directly to the `ModelHandler`. The model handler coordinates the rate limit internally across your distributed workers.
{{< /paragraph >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/inference/rate_limiter_vertex_ai.py" RateLimiterVertexPy >}}
{{< /highlight >}}

---

## Running Example Pipelines with RateLimiter

Once your Rate Limiter Service is deployed and has an Internal IP, you can run your pipeline pointing to that address.

{{< highlight java >}}
# Get the IP from your RLS deployment
export RLS_ADDRESS="<INTERNAL_IP>:8081"

./gradlew :examples:java:exec -DmainClass=org.apache.beam.examples.RateLimiterSimple \
  -Dexec.args="--runner=<RUNNER> \
  --rateLimiterAddress=${RLS_ADDRESS} \
  --rateLimiterDomain=mongo_cps"
{{< /highlight >}}

{{< highlight py >}}
# Get the IP from your RLS deployment
export RLS_ADDRESS="<INTERNAL_IP>:8081"

python -m apache_beam.examples.rate_limiter_simple \
  --runner=<RUNNER> \
  --rls_address=${RLS_ADDRESS}
{{< /highlight >}}

## AutoScaler Integration

The throttling time and signals from the RateLimiter has to be picked up by the autoscaler. This allows the autoscaler to scale down the workers when the pipeline is being throttled by the external service, preventing unnecessary resource usage.

**Dataflow** currently supports this AutoScaler integration for **Batch RunnerV2**. Note that AutoScaler integration for Streaming mode is a known limitation.
