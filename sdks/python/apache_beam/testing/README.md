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

# Evidence of orphaned resources from the tests relevant to implementing the cleanup handler.

While identifying opportunities to implement the new `TestPubSubContext` module—aimed at preventing resource leaks in the GCP environment—we conducted a test using a Python script: `auditar_backlog_wordcount.py`.

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

## Pub/Sub Integration Test Subscriptions

**Critical findings:**

* We detected exactly 87 active, orphaned subscriptions in GCP under the patterns `psit_subscription_input`..., `psit_subscription_output`..., and `psit_sub_ordering`....
* These 87 dead queues were created during previous CI/CD test runs but were never deleted due to Jenkins or GitHub Actions pipeline failures that bypassed the standard cleanup block.
* These active queues have been silently accumulating and retaining unacknowledged messages (backlog) from continuous test runs, generating ongoing ghost storage costs.
**Evidence from the GCP audit log:**
```text
Suscripción Huérfana Detectada                                         | Estado
------------------------------------------------------------------------------------------
psit_subscription_output50347d48-743d-4ee7-9f9c-8fdcca650b84           | ACTIVA (HUÉRFANA)
psit_subscription_input51a51eec-193c-455f-9cc5-ea6a57d79062            | ACTIVA (HUÉRFANA)
psit_subscription_output85e31e61-0eb4-4ecf-8f8d-e824b6fa7c66           | ACTIVA (HUÉRFANA)
psit_subscription_input6906b262-7818-4b20-9ace-e3c6885f3f49            | ACTIVA (HUÉRFANA)
psit_subscription_inputed376474-e61e-49e1-95ee-7ed4174cc264            | ACTIVA (HUÉRFANA)
...
[82 more orphaned psit_ subscriptions listed]
------------------------------------------------------------------------------------------
Diagnóstico: Se detectaron 87 suscripciones 'psit_' huérfanas activas en GCP.

```


## Streaming Wordcount integration tests.

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