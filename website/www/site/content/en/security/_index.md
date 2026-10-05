---
title: "Beam Security"
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

# Apache Beam security

Apache Beam is a programming model to author user-supplied code as stream and batch processing pipelines in a supported distributed system.

## Pipeline lifecycle

A Beam pipeline's full lifecycle involves two environments: the **pipeline submission environment** and the **pipeline execution environment**.

**Pipeline submission environment** - This is the environment where the user submits the pipeline to the runner. The pipeline is constructed (compiled/expanded) in this stage and the execution graph is created. The pipeline graph and dependencies are uploaded to a staging location.

**Pipeline execution environment** - This is the environment where the pipeline is executed by the runner. The staged graph and dependencies are downloaded from the staging location to the workers' local storage and the pipeline is executed.

## Trust Boundary

**Authenticated users who submit jobs are fully trusted.** Any components or services invoked during pipeline construction time assume that the pipeline options, transforms, and other configurations are from a trusted user.

Based on the above assumption, the following example scenarios are not considered security vulnerabilities:

- Remote code execution in an Expansion Service: An expansion service is used to construct a portable pipeline that may have pre-defined stages, and is invoked during pipeline construction time in the pipeline submission environment. Because the transform configuration is from a trusted user, it is assumed that the expansion service user is a trusted entity.

- SQL injection based on transform configuration options at pipeline submission time: For example, Beam JdbcIO provides the interface `withQuery(...)` that accepts any query. It is possible for a transform based on `JdbcIO.withQuery` to be used to invoke SQL commands with injected code, but because this is an intended usage of `withQuery(...)` and the transform configuration is from a trusted user, this scenario is not considered a security vulnerability.

**Unauthenticated access to workers at pipeline execution time is the threat the runner protects against.** Beam pipelines run on a supported runner, and it is the runner architecture that protects against unauthenticated access to workers at pipeline execution time. See the following runner security models for examples:

* [**Dataflow Runner**](/documentation/runners/dataflow/): https://docs.cloud.google.com/dataflow/docs/concepts/security-and-permissions

* [**Flink Runner**](/documentation/runners/flink/): https://flink.apache.org/what-is-flink/security/

* [**Spark Runner**](/documentation/runners/spark/): https://spark.apache.org/docs/latest/security.html

Beam also provides local runners:

* Java Direct Runner; Python FnApiRunner: These runners are for local debugging purposes and are not intended for any production use. These runners run in an in-process JVM or Python process and do not have a security model defined.

* [**Prism Runner**](/documentation/runners/prism/): A single-node runner that can be run standalone for local development and testing. When running in standalone mode, Prism exposes a gRPC JobManagement endpoint and an HTTP Web UI (bound to `localhost` by default) without built-in authentication, authorization, or TLS encryption, and executes submitted pipeline code on the host machine (via local processes or Docker containers) with the privileges of the Prism process. Users are expected to run Prism in a trusted local environment and must not expose its endpoints to untrusted networks. However, vulnerabilities in Prism that allow unintended remote access, cross-site scripting (XSS)/cross-site request forgery (CSRF) via the Web UI, or unauthorized host file access beyond intended job execution are in scope.

**The staging location is trusted and protecting it is the user's responsibility.** The staging location is referenced across runners. It acts as the storage for pipeline logic and dependencies. While Beam provides file integrity checks for staged artifacts, it is the user's responsibility to protect access to the staging location.

## Security boundary reference

The table below is intended for security researchers and enterprise security teams evaluating Beam:

<div style="font-size: 16px;">
{{< table class="table-wrapper--equal-p" >}}
| Scenario | Security boundary | Notes |
| --- | --- | --- |
| Unauthenticated access to the runner worker | Runner’s security model | Depends on the runner; report to the runner provider |
| Unauthenticated access to the host machine using Prism Runner | In scope | Prism runner has a basic security model |
| Code execution via unsafe deserialization of input data where no Beam-level control exists to prevent it | In scope | Vulnerability – report it |
| SQL injection via unsafe parsing of input data where no Beam-level control exists to prevent it | In scope | Vulnerability – report it |
| Existing nomenclature, documentation, and logic expose risk of supply-chain attack | **Depends** | See notes below |
| Denial of service (DoS) via certain data patterns where no Beam-level control exists to prevent it | In scope | Vulnerability – report it, with exceptions (see notes below) |
| Remote Code Execution (RCE) via a submitted JAR, Expansion service, or UDF | Out of scope | By design – these submitters run arbitrary code |
{{< /table >}}
</div>

**Notes**:

* Based on previous practices, Beam generally avoids imposing new hard size limits on existing functionality to protect against DoS attacks caused by unbounded data. In fact, similar restrictions introduced in upstream dependencies have caused regressions for intended use cases of Beam (e.g., [#31580](https://github.com/apache/beam/pull/31580)). Since Beam is widely used for big data processing, arbitrary size limits risk causing regressions.

* For supply-chain risks: If active Beam release artifacts, build scripts, runtime logic (such as default container image or binary resolution), or current official documentation reference or fetch from an unclaimed, expired, or hijackable external resource (for example, an unclaimed package name, domain, or cloud storage bucket), it is **in scope** and should be reported. Conversely, risks requiring a user to explicitly configure an untrusted third-party repository or container image, typosquatting on external registries outside the project's control, or references found only in archived documentation or unsupported versions are **out of scope**.

## Reporting Security Issues

Apache Beam uses the standard process outlined by the [Apache Security
Team](https://www.apache.org/security/) for reporting vulnerabilities. Note
that vulnerabilities should not be publicly disclosed until the project has
responded.

Before reporting a possible security vulnerability, please review this page to check if the vulnerability pattern is explicitly out of scope or should be routed to the runner provider. To report a possible security vulnerability, please email
`security@apache.org` and `pmc@beam.apache.org`. This is a non-public list
that will reach the Beam PMC.

## Known Security Issues

For security issues in old Beam versions, see [archive](/security/archive/).
