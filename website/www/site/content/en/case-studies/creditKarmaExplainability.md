---
title: "Credit Karma: Scaling ML Model Explainability as a Platform Capability"
name: "Credit Karma: Beam for ML Explainability"
icon: /images/logos/powered-by/credit-karma.png
hasNav: true
category: study
cardTitle: "Credit Karma: Scaling ML Model Explainability as a Platform Capability"
cardDescription: "Apache Beam lets us build explainability once and run it across every model in our portfolio. We own the execution, the debuggability, and the scale — without paying for idle infrastructure between runs."
authorName: "Pallav Anand"
coauthorName: "Raj Katakam"
authorPosition: "Staff Machine Learning Engineer @ Credit Karma"
coauthorPosition: "Sr Staff Machine Learning Engineer @ Credit Karma"
authorImg: /images/case-study/credit_karma/pallav_anand.jpeg
coauthorImg: /images/case-study/credit_karma/raj_katakam.jpeg
publishDate: 2026-11-03T00:12:00+00:00
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
<!-- div with class case-study-opinion is displayed at the top left area of the case study page -->
<div class="case-study-opinion">
    <div class="case-study-opinion-img">
        <img src="/images/logos/powered-by/credit-karma.png"/>
    </div>
    <blockquote class="case-study-quote-block">
      <p class="case-study-quote-text">
        We built portfolio-wide explainability on Beam by drawing a clean line between teams: Data Scientists can update their feature groupings without touching our code, and we can swap explainability methods without touching theirs. That clean boundary is what lets a platform scale across teams, not just data volumes.
      </p>
      <div class="case-study-quote-author">
        <div class="case-study-quote-author-img">
            <img src="/images/case-study/credit_karma/pallav_anand.jpeg">
        </div>
        <div class="case-study-quote-author-info">
            <div class="case-study-quote-author-name">
              Pallav Anand
            </div>
            <div class="case-study-quote-author-position">
              Staff Machine Learning Engineer, Credit Karma
            </div>
        </div>
      </div>
    </blockquote>
</div>

<!-- div with class case-study-post is the case study page main content -->
<div class="case-study-post">

# Credit Karma: Scaling ML Model Explainability as a Platform Capability

## Background

Intuit Credit Karma is a financial management platform that helps more than 140 million members improve their financial lives through personalized recommendations, insights, and tools powered by data and AI. As Credit Karma's ML model portfolio expanded — spanning financial product recommendations, fraud detection, underwriting, and member engagement — the need to explain individual model predictions became critical. Business stakeholders needed to understand why models made specific decisions. Members need better transparency into financial opportunities. Data science teams needed feature-level attribution for model diagnostics, drift detection, and continuous improvement.

Rather than solving this once per model, Credit Karma's Machine Learning Infrastructure (MLI) team built a reusable model explainability platform on Apache Beam, running on Google Cloud Dataflow — designed to plug into any model in the portfolio and deliver feature-level explanations at scale.

## Journey to Apache Beam

Credit Karma evaluated multiple approaches for running explainability at scale before landing on Apache Beam. A serving-oriented architecture — where instances are sent to a hosted endpoint and explanations are returned — is a natural fit for online, low-latency use cases, but it introduces friction for a batch workload with specific requirements:

- **Batch economics**: Endpoint-based infrastructure is sized for peak request rates, not batch throughput. An explainability job has a predictable, time-bounded compute need — paying for persistent serving capacity around the clock doesn't match that shape.
- **Ownership and extensibility**: Credit Karma's explainability requirements vary across model types and verticals. An owned pipeline makes it straightforward to integrate different attribution methods, support new model formats, and evolve the logic as Data Science needs change — without being constrained by the capabilities of an external API surface.
- **Debuggability**: Batch pipelines provide full visibility into execution — which instances were processed, where failures occurred, what the intermediate outputs looked like. This level of observability is essential when explanation correctness directly affects downstream business workflows.
- **Existing infrastructure fit**: Credit Karma already ran Dataflow-based ML scoring and ETL pipelines extensively. Reusing the same execution model, monitoring tooling, and GCP integration reduced operational overhead significantly.

Apache Beam on Cloud Dataflow addressed all of these. The pipeline runs on-demand, scales horizontally with the batch, and imposes no restrictions on which attribution method or library is used per model type.

<blockquote class="case-study-quote-block case-study-quote-wrapped">
  <p class="case-study-quote-text">
    Always-on endpoints charge you whether or not you're computing, and give you almost no visibility when things go wrong. With Apache Beam on Dataflow, we own the execution, we own the debuggability, and the pipeline scales with the work — not against it.
  </p>
  <div class="case-study-quote-author">
    <div class="case-study-quote-author-img">
        <img src="/images/case-study/credit_karma/raj_katakam.jpeg">
    </div>
    <div class="case-study-quote-author-info">
        <div class="case-study-quote-author-name">
          Raj Katakam
        </div>
        <div class="case-study-quote-author-position">
          Sr Staff Machine Learning Engineer, Credit Karma
        </div>
    </div>
  </div>
</blockquote>

## Use Cases

Credit Karma runs hundreds of ML models across its platform — powering products like Approval Odds (estimating a member's likelihood of being approved for a financial offer), personalized product recommendations, real-time financial insights, and credit score monitoring. Each vertical has its own data science team, its own model refresh cadence, and its own definition of what a "good explanation" looks like for their use case.

The explainability platform was built to serve all of them without requiring MLI to become experts in each domain. Each team provides two artifacts — a feature log table and a DS-owned reason code config — and the pipeline handles the rest, producing a standardized attribution output in BigQuery regardless of model type or explainability method. Onboarding a new model is a configuration exercise, not an engineering project.

## A Use Case in Focus: Multi-Model Explainability with AMR Integration

A key requirement across all use cases was **model version fidelity**. Credit Karma uses an Automated Model Refresh (AMR) system that retrains and promotes models on a cadence. An explainability job must use the exact model version that generated the original online prediction — not the latest promoted version. Using the wrong version produces different attribution values, undermining explanation accuracy.

Apache Beam's DoFn model handled this cleanly: instances are grouped by model version identifier at runtime, each group loads its matching model artifact from the AMR store in GCS, and attribution is computed against that specific artifact. New model promotions require no changes to the pipeline — the version-aware loading logic picks up whatever AMR produces automatically.

The pipeline processes feature logs stored as BigQuery tables, with structured data including full feature vectors, online prediction scores, and model version identifiers per instance. Attribution outputs are written back to BigQuery in a consistent {feature\_group, feature\_name, attribution\_value} schema regardless of which model or explainability method was used — enabling downstream consumers to query explanation data uniformly across the entire model portfolio.

## Platform Architecture

<div class="post-scheme">
    <a href="/images/case-study/credit_karma/credit_karma_explainability.png" target="_blank" title="Click to enlarge">
        <img src="/images/case-study/credit_karma/credit_karma_explainability.png" alt="Credit Karma model explainability platform architecture">
    </a>
</div>

**Stage 1 — Input preparation**: Reads from BigQuery feature log tables containing each scored member's feature vector, online prediction score, model version identifier, and decision outcome. A lightweight transform normalizes the schema for the attribution stage.

**Stage 2 — Feature attribution**: Groups instances by model version, retrieves the matching model artifact from GCS, and runs the appropriate explainer — method selected per model type. Beam's DoFn execution model parallelizes this across Dataflow workers automatically. Output is a feature\_info struct with {feature\_group, feature\_name, attribution\_value} per feature, consistent across all models.

**Stage 3 — Reason code aggregation**: Applies a DS-owned config mapping 1,200+ raw features into 10–15 human-interpretable reason code groups using configurable aggregation functions. MLI owns the attribution step; Data Science owns the grouping logic — neither team needs to touch the other's code.

**Stage 4 — Inline output validation**: Before writing output, the pipeline enforces correctness checks — probability parity between offline and online scores, no null predictions, valid base values from the explainer, non-trivial reason code output. If any check fails, the pipeline halts without writing, preventing incorrect explanation data from propagating downstream.

## Results

- **Explainability across the full model portfolio** — a single platform serves every model regardless of type, framework, or vertical, with no per-model MLI engineering required
- **~2 hour end-to-end runtime** for a full batch, well within the 6-hour operational target
- **Zero idle infrastructure cost** — Dataflow workers spin up and down with each job; no capacity reserved between runs
- **Automatic AMR compatibility** — model refreshes require no pipeline changes; version-aware loading handles new promotions automatically
- **Full execution visibility** — Dataflow's job graph and Cloud Monitoring give complete observability into attribution throughput, worker utilization, and failure modes

## What's Next

- **Next-gen ML platform integration**: Porting the pipeline to a reusable containerized component in Credit Karma's next-generation ML training platform — making explainability a plug-in step in any model's training and scoring workflow
- **Broader explainer coverage**: Expanding supported attribution methods to cover additional model architectures across the portfolio
- **Online explainability**: Exploring real-time explanation serving as model serving infrastructure matures

## Summary

Credit Karma's explainability platform demonstrates how Apache Beam's execution model maps naturally onto complex, multi-tenant ML workloads. By owning the pipeline rather than delegating to a managed endpoint, the MLI team gained the flexibility to support any model type, the debuggability to enforce correctness at scale, and the economics to run without idle cost. The result is a platform that grows with Credit Karma's model portfolio rather than constraining it.

**Tech stack**: Apache Beam · Google Cloud Dataflow · Python · BigQuery · Google Cloud Storage

<!-- case_study_feedback adds feedback buttons -->
{{< case_study_feedback "CreditKarmaExplainability" >}}

</div>
<div class="clear-nav"></div>
