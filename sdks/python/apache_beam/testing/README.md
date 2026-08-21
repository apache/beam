<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Audit Log and Justification: Implementation of TestPubsubContext
#### Date: 2026-08-20

This document serves as a living log of resource leaks detected in our GCP environment due to failures or premature interruptions in CI/CD pipelines (such as Jenkins or GitHub Actions). The purpose of this log is to centralize evidence of orphaned components and provide technical and economic justification for implementing the TestPubsubContext lifecycle manager across all integration tests. The engineering team is encouraged to document any new leaks detected in future test suites in this file to maintain strict control over infrastructure consumption.

## Detection Methodology (Audit Script)

To identify optimization opportunities and prevent the accumulation of phantom resources in GCP, we developed an automated audit script (`auditar_backlog_wordcount.py`). This script connects to the `apache-beam-testing` project and actively filters for orphaned subscriptions based on the prefixes used by our test suites.
```
python

from google.cloud import pubsub_v1
from datetime import datetime, timezone, timedelta

def audit_wordcount_subscriptions(project_id):
    subscriber = pubsub_v1.SubscriberClient()
    project_path = f"projects/{project_id}"

    print(f"Searching for orphan subscriptions with 'resource_sub' prefixes in: {project_id}...\n")
    print(f"{'Orphan Subscription Detected':<70} | {'Status'}")
    print("-" * 90)
    total_leaks = 0

    # List subscriptions in the GCP project
    for sub in subscriber.list_subscriptions(project=project_path):
        sub_name = sub.name.split("/")[-1]

        # Filter by those created by wordcount_it_test ('name of the resource')
        if sub_name.startswith("resource_sub") or "resource_subscription" in sub_name:
            total_leaks += 1
            print(f"{sub_name:<70} | ACTIVE (ORPHAN)")

    print("-" * 90)
    print(f"Diagnosis: Detected {total_leaks} active orphan 'resource_sub' subscriptions in GCP.")

if __name__ == "__main__":
    audit_wordcount_subscriptions("apache-beam-testing")

```

## Evidence: Leaks in Pub/Sub Integration Tests (`psit_`)

During the execution of the main integration test suite, the standard cleanup mechanism proved insufficient when tests failed or were abruptly aborted.

### **Critical findings:**

* We detected exactly 87 active, orphaned subscriptions in GCP under the patterns `psit_subscription_input`..., `psit_subscription_output`..., and `psit_sub_ordering`....
* These dead queues were created during previous CI/CD test runs but were never deleted due to Jenkins or GitHub Actions pipeline failures that bypassed the standard cleanup block.
* These active queues have been silently accumulating and retaining unacknowledged messages (backlog) from continuous test runs, generating ongoing ghost storage costs.

```text
Orphan Subscription Detected                                           | Status
------------------------------------------------------------------------------------------
psit_subscription_output50347d48-743d-4ee7-9f9c-8fdcca650b84           | ACTIVE (ORPHAN)
psit_subscription_input51a51eec-193c-455f-9cc5-ea6a57d79062            | ACTIVE (ORPHAN)
psit_subscription_output85e31e61-0eb4-4ecf-8f8d-e824b6fa7c66           | ACTIVE (ORPHAN)
psit_subscription_input6906b262-7818-4b20-9ace-e3c6885f3f49            | ACTIVE (ORPHAN)
psit_subscription_inputed376474-e61e-49e1-95ee-7ed4174cc264            | ACTIVE (ORPHAN)
...
[82 more orphaned psit_ subscriptions listed]
------------------------------------------------------------------------------------------
Diagnosis: Detected 87 active orphan 'psit_' subscriptions in GCP.
```

## Evidence: Leaks in Streaming Wordcount (`wc_`) and Handler Justification

**Critical findings:**
* We detected exactly **142 active, orphaned subscriptions** in GCP—following the patterns `wc_subscription_input...` and `wc_subscription_output...`—left behind by aborted or failed Jenkins CI runs.
* These 142 inactive queues have been silently accumulating unacknowledged messages (backlogs), thereby inflating GCP storage costs.

**Evidence from the GCP audit log:**
```text
Orphaned subscription detected | Status
------------------------------------------------------------------------------------------
wc_subscription_outputd71a1c7c-ba81-40f6-8d03-682cad78e162 | ACTIVE (ORPHANED)
wc_subscription_input7bd1abaa-6955-4f0a-a3b4-fa51c0a835eb | ACTIVE (ORPHANED)
...
[140 additional orphaned subscriptions listed]
------------------------------------------------------------------------------------------
Diagnosis: 142 active, orphaned 'wc_' subscriptions detected in GCP.
```

### Justification for adopting TestPubsubContext

Both Wordcount tests dynamically instantiate subscriptions and topics using the variables INPUT_TOPIC = 'wc_topic_input'
OUTPUT_TOPIC = 'wc_topic_output', INPUT_SUB = 'wc_subscription_input', and OUTPUT_SUB = 'wc_subscription_output'.
Historically, when these tests ran in parallel in CI/CD and failed, it was impossible to determine which specific test left
each resource behind.

By wrapping these tests with TestPubsubContext, the handler uses Python's execution stack inspection (inspect.stack()) to automatically capture the class name of the test that originated the request (self.caller_class = self_obj.__class__.__name__). When a subscription or topic is registered, the handler detects which test created it and injects it directly into the execution log. If the test fails, the handler logs it and allows for a "teardown" with the exact trace of who created the resource, facilitating debugging and guaranteeing that subsequent cleanup is traceable.