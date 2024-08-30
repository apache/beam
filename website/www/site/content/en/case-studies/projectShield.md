---
title: "Efficient Streaming Analytics: Making the Web a Safer Place with Project Shield"
name: "Project Shield"
icon: "/images/logos/powered-by/project_shield.png"
category: "study"
cardTitle: "Efficient Streaming Analytics: Making the Web a Safer Place with Project Shield"
cardDescription: "Project Shield defends the websites of over 3K vulnerable organizations in >150 countries against DDoS attacks with the mission of protecting freedom of speech. The Apache Beam streaming pipelines process about 3 TB of log data daily at >10,000 queries per second. The pipelines produce real-time user-facing analytics, tailored traffic rate limits, and defense recommendations. Apache Beam enabled the delivery of critical metrics at scale with a ~2x efficiency gain. This data supported Project Shield’s goal of eliminating the DDoS attack as a weapon for silencing the voices of journalists and others who speak the truth. Ultimately, Project Shield’s goal is to make the web a safer place."
authorName: "Marc Howard"
coauthorName: "Chad Hansen"
authorPosition: "Founding Engineer @ Project Shield"
coauthorPosition: "Founding Engineer @ Project Shield"
authorImg: /images/case-study/projectShield/marc_howard.jpg
coauthorImg: /images/case-study/projectShield/chad_hansen.png
publishDate: 2023-06-08T00:12:00+00:00
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
<div class="case-study-opinion">
    <div class="case-study-opinion-img">
        <img src="/images/logos/powered-by/project_shield.png"/>
    </div>
    <blockquote class="case-study-quote-block">
      <p class="case-study-quote-text">
        “Apache Beam supports our mission to make the web a safer and better place by providing near-real-time visibility into traffic data for our customers, providing ongoing analysis and adjustments to our defenses, and neutralizing the impact of traffic spikes during DDoS attacks on the performance and efficiency of our platform.”
      </p>
      <div class="case-study-quote-author">
        <div class="case-study-quote-author-img">
            <img src="/images/case-study/projectShield/marc_howard.jpg">
        </div>
        <div class="case-study-quote-author-info">
            <div class="case-study-quote-author-name">
              Marc Howard
            </div>
            <div class="case-study-quote-author-position">
              Founding Engineer @ Project Shield
            </div>
        </div>
      </div>
    </blockquote>
</div>
<div class="case-study-post">

# Efficient Streaming Analytics: Making the Web a Safer Place with Project Shield

## Background

[Project Shield](https://projectshield.withgoogle.com/landing), offered by [Google Cloud](https://cloud.google.com/) and [Jigsaw](https://jigsaw.google.com/) (a subsidiary of Google), is a service that counters [distributed-denial-of-service](https://en.wikipedia.org/wiki/Distributed-denial-of-service) (DDoS) attacks. Project Shield is available free of charge to eligible websites that have media, elections, and human rights related content. [Founded in 2013](https://www.forbes.com/sites/andygreenberg/2013/10/21/googles-project-shield-will-offer-free-cyberattack-protection-to-hundreds-of-at-risk-sites/), Project Shield’s mission is to protect freedom of speech and to make sure that, when people have access to democracy-related information, the information isn’t compromised, censored, or silenced in a politically-motivated way.

In the first half of 2022, Project Shield defended websites of vulnerable users - such as human rights, news, and civil society organizations or governments under exigent circumstances - [against more than 25,000 attacks](https://cloud.google.com/blog/products/identity-security/ddos-attack-trends-during-us-midterm-elections). Notably, Project Shield helped ensure [unhindered access to election-related information during the U.S. 2022 midterm election season](https://cloud.google.com/blog/products/identity-security/ddos-attack-trends-during-us-midterm-elections). It also [enables Ukrainian critical infrastructure and media websites to defend against non-stop attacks](https://cio.economictimes.indiatimes.com/news/internet/google-expands-project-shield-to-protect-govts-from-hacking/90072091) and to continue providing crucial services and information during the invasion of Ukraine.

Marc Howard and Chad Hansen, the co-founding engineers, explain how Project Shield uses Apache Beam to deliver some of their core value. The streaming Apache Beam pipelines process about 3 TB of log data daily at significantly over 10,000 queries per second. These pipelines enable multiple product needs. For example, Apache Beam produces real-time analytics and critical metrics for [over 3000 customer websites in 150 countries](https://www.washingtonpost.com/opinions/2022/06/21/russia-ukraine-cyberwar-intelligence-agencies-tech-companies/). These metrics power long-term attack analytics at scale, fine-tuning Project Shield’s defenses and supporting them in the effort of making the web a safe and free space.

## Journey To Beam

The Project Shield platform is built using Google Cloud technologies and provides multi-layered defenses. To absorb part of the traffic and keep websites online even if their servers are down, it uses [Cloud CDN](https://cloud.google.com/cdn) for [caching](https://en.wikipedia.org/wiki/Cache_(computing)). To protect websites from DDoS and other malicious attacks, it leverages [Cloud Armor](https://cloud.google.com/armor) features, such as adaptive protection, rate limiting, and bot management.

Project Shield acts as a [reverse proxy](https://en.wikipedia.org/wiki/Reverse_proxy): it receives traffic requests on a website’s behalf, absorbs traffic through caching, filters harmful traffic, bans attackers, and then sends safe traffic to a website’s origin server. This configuration allows websites to stay up and running when someone tries to take them down with a DDoS attack. The challenge is that, with a large portion of traffic blocked, the logs on customers’ origin servers no longer have accurate analytics about website traffic. Instead, customers rely on Project Shield to provide all of the traffic analytics.

<div class="post-scheme">
    <a href="/images/case-study/projectShield/project_shield_mechanism.png" target="_blank" title="Click to enlarge">
        <img src="/images/case-study/projectShield/project_shield_mechanism.png" alt="Project Shield Mechanism">
    </a>
    <span>Project Shield Mechanism</span>
</div>

Originally, Project Shield stored traffic logs in [BigQuery](https://cloud.google.com/bigquery). It used one large query to produce analytics and charts with different traffic metrics, such as the amount of traffic, QPS, the share of cached traffic, and the attack data. However, querying all of the logs, especially with dramatic spikes in traffic, was slow and expensive.

<blockquote class="case-study-quote-block case-study-quote-wrapped">
  <p class="case-study-quote-text">
    Often people want to know traffic metrics during critical times: if their website is under attack: they want to see what's happening right now. We need the data points to appear on the UI fast.
  </p>
  <div class="case-study-quote-author">
    <div class="case-study-quote-author-img">
        <img src="/images/case-study/projectShield/marc_howard.jpg">
    </div>
    <div class="case-study-quote-author-info">
        <div class="case-study-quote-author-name">
          Marc Howard
        </div>
        <div class="case-study-quote-author-position">
          Founding Engineer @ Project Shield
        </div>
    </div>
  </div>
</blockquote>

Project Shield’s team then added [Firestore](https://firebase.google.com/docs/firestore) as an intermediate step, running a [cron](https://en.wikipedia.org/wiki/Cron) every minute to query logs from BigQuery and write the interim reports to Firestore, then using these reports to build charts. This step improved performance slightly, but the gain was not sufficient to meet critical business timelines, and it didn’t provide adequate visibility into historical traffic for customers.

Unlike BigQuery, Firestore was designed for medium-sized workloads. Therefore, it wasn’t possible to fetch many models at the same time to provide customers with cumulative statistics for extended time frames. Querying the logs every minute was inefficient from a cost perspective. In addition, some of the logs were coming to BigQuery with a delay, and it was necessary to run the cron again in 24 hours to double-check for the late-coming logs.

<blockquote class="case-study-quote-block case-study-quote-wrapped">
  <p class="case-study-quote-text">
    Querying over all of our traffic logs every minute is very expensive, especially when you consider that we are a DDoS defense service - the number of logs that we see can often spike dramatically.
  </p>
  <div class="case-study-quote-author">
    <div class="case-study-quote-author-img">
        <img src="/images/case-study/projectShield/marc_howard.jpg">
    </div>
    <div class="case-study-quote-author-info">
        <div class="case-study-quote-author-name">
          Marc Howard
        </div>
        <div class="case-study-quote-author-position">
          Founding Engineer @ Project Shield
        </div>
    </div>
  </div>
</blockquote>

Project Shield’s team looked for a data processing framework that would minimize end-to-end latency, meet their scaling needs for better customer visibility, and ensure cost efficiency.

They selected Apache Beam for its [strong processing guarantees](/documentation/runtime/model/), streaming capabilities, and out-of-the-box [I/Os](/documentation/io/connectors/) for BigQuery and Pub/Sub. By pairing Apache Beam with the [Dataflow runner](/documentation/runners/dataflow/), they also benefited from built-in autoscaling. In addition, the [Apache Beam Python SDK](/documentation/sdks/python/) lets you use Python across the board and simplifies reading data model types that have to be the same on the backend that consumes them and on the pipeline that writes them.

## Use Cases

Project Shield became one of the early adopters of Apache Beam and migrated their workflows to streaming Apache Beam pipelines in 2020. Currently, Apache Beam powers multiple product needs with multiple streaming pipelines.

The unified streaming pipeline that produces user-facing analytics reads the logs from Pub/Sub while they arrive from Cloud Logging, windows logs per minute every minute, splits by the hostname of the request, generates reports, and writes the reports to BigQuery. The pipeline aggregates log data, removes [Personally Identifiable Information](https://en.wikipedia.org/wiki/Personal_data) (PII) without using a [DLP](https://en.wikipedia.org/wiki/Data_loss_prevention_software), and allows for storing the data in BigQuery for a longer period while meeting the regulatory requirements. [CombineFn](/documentation/transforms/python/aggregation/combineperkey/#example-5-combining-with-a-combinefn) allows Project Shield’s team to create a custom accumulator that takes the key into account when keying log data off the hostname and minute, [combining the log data per key](/documentation/programming-guide/#combine). If logs come in late, Apache Beam creates a new report and aggregates several reports per hostname per minute.

<blockquote class="case-study-quote-block case-study-quote-wrapped">
  <p class="case-study-quote-text">
    The fact that we can just key data, use CombinePerKey, and the accumulator works like magic was a big win for us
  </p>
  <div class="case-study-quote-author">
    <div class="case-study-quote-author-img">
        <img src="/images/case-study/projectShield/marc_howard.jpg">
    </div>
    <div class="case-study-quote-author-info">
        <div class="case-study-quote-author-name">
          Marc Howard
        </div>
        <div class="case-study-quote-author-position">
          Founding Engineer @ Project Shield
        </div>
    </div>
  </div>
</blockquote>

The Apache Beam log processing pipeline provides Project Shield with the ability to query only the relevant reports, thus enabling Project Shield to load the data to the customer dashboard within just 2 minutes. The pipeline also provides enhanced visibility for Project Shield’s customers, because the queried reports are much smaller in size and easier to store than the traffic logs.

<blockquote class="case-study-quote-block case-study-quote-wrapped">
  <p class="case-study-quote-text">
    The end-to-end pipeline latency was very meaningful to us, and the Apache Beam streaming allowed for displaying all the traffic metrics on charts within 2 minutes, but also to look back on days, weeks, or months of data and show graphs on a scalable timeframe to customers.
  </p>
  <div class="case-study-quote-author">
    <div class="case-study-quote-author-img">
        <img src="/images/case-study/projectShield/chad_hansen.png">
    </div>
    <div class="case-study-quote-author-info">
        <div class="case-study-quote-author-name">
          Chad Hansen
        </div>
        <div class="case-study-quote-author-position">
          Founding Engineer @ Project Shield
        </div>
    </div>
  </div>
</blockquote>

Project Shield extended the streaming Apache Beam log processing pipeline to generate a different type of report based on the logs and requests during attacks. The Apache Beam pipeline analyzes attacks and generates critical defense recommendations. These recommendations are then used by the internal long-term attack analysis system to fine-tune Project Shield’s defenses.

Apache Beam also powers Project Shield’s traffic rate-limiting decisions by analyzing patterns in the traffic logs. The streaming Apache Beam pipeline gathers information about the legitimate usage of a website, excludes abusive traffic from that analysis, and crafts traffic rate limits that divide the two groups safely. Those limits are then enforced in the form of Cloud Armor rules and policies and used to restrict abusive traffic or to ban it entirely.

<div class="post-scheme">
    <a href="/images/case-study/projectShield/apache_beam_streaming_log_analytics.png" target="_blank" title="Click to enlarge">
        <img src="/images/case-study/projectShield/apache_beam_streaming_log_analytics.png" alt="Apache Beam Streaming Log Analytics">
    </a>
    <span>Apache Beam Streaming Log Analytics</span>
</div>

The combination of Apache Beam and Cloud Dataflow greatly simplifies Project Shield’s operational management of streaming pipelines. Apache Beam provides easy-to-use streaming primitives, while Dataflow enables [out-of-the-box pipeline lifecycle management](https://cloud.google.com/dataflow/docs/pipeline-lifecycle) and compliments Pub/Sub’s delivery model with [message deduplication and exactly-once, in-order processing](https://www.cloudskillsboost.google/focuses/18457?parent=catalog). The Apache Beam [Pub/Sub I/O](https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.pubsub.html) provides the ability to key log data off Pub/Sub event timestamps. This feature enables Project Shield to improve the overall data accuracy by windowing log data after all the relevant logs come in. [Various options for managing the pipeline lifecycle](https://cloud.google.com/dataflow/docs/guides/pipeline-workflows#update-streaming-pipelines-prod) allow Project Shield to employ simple and reliable deployment processes. The Apache Beam Dataflow runner’s [autoscaling and managed service capabilities](https://cloud.google.com/dataflow/docs/horizontal-autoscaling) help handle dramatic spikes in resource consumption during DDoS attacks and provide instant visibility for customers.

<blockquote class="case-study-quote-block case-study-quote-wrapped">
  <p class="case-study-quote-text">
    When attacks come in, we are ready to handle high volumes of traffic and deliver on-time metrics during critical windows with Apache Beam.
  </p>
  <div class="case-study-quote-author">
    <div class="case-study-quote-author-img">
        <img src="/images/case-study/projectShield/chad_hansen.png">
    </div>
    <div class="case-study-quote-author-info">
        <div class="case-study-quote-author-name">
          Chad Hansen
        </div>
        <div class="case-study-quote-author-position">
          Founding Engineer @ Project Shield
        </div>
    </div>
  </div>
</blockquote>

## Results

The adoption of Apache Beam enabled Project Shield to embrace streaming, scale its pipelines, maximize efficiency, and minimize latency. The streaming Apache Beam pipelines process about 3 TB of log data daily at significantly over 10,000 queries per second to produce user-facing analytics, tailored traffic rate limits, and defense recommendations for over 3K customers all over the world. The streaming processing and powerful transforms of Apache Beam ensure delivery of critical metrics within just 2 minutes, enabling customer visibility into historical traffic, and resulting in a 91% compute efficiency gain, compared to the original solution.

<blockquote class="case-study-quote-block case-study-quote-wrapped">
  <p class="case-study-quote-text">
    The Apache Beam model and the autoscaling capabilities of its Dataflow runner help prevent the spikes in traffic during DDoS attacks from having a meaningful impact on our platform from an efficiency and financial perspective.
  </p>
  <div class="case-study-quote-author">
    <div class="case-study-quote-author-img">
        <img src="/images/case-study/projectShield/marc_howard.jpg">
    </div>
    <div class="case-study-quote-author-info">
        <div class="case-study-quote-author-name">
          Marc Howard
        </div>
        <div class="case-study-quote-author-position">
          Founding Engineer @ Project Shield
        </div>
    </div>
  </div>
</blockquote>

The Apache Beam data processing framework supports Project Shield’s goal to eliminate the DDoS attack as a weapon for silencing the voices of journalists and others who speak the truth, making the web a safer place.

{{< case_study_feedback "Project Shield" >}}

</div>
<div class="clear-nav"></div>
