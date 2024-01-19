---
title: "High-Performing and Efficient Transactional Data Processing for OCTO Technology’s Clients"
name: "OCTO"
icon: "/images/logos/powered-by/octo.png"
category: "study"
cardTitle: "High-Performing and Efficient Transactional Data Processing for OCTO Technology’s Clients"
cardDescription: "With Apache Beam, OCTO accelerated the migration of one of France’s largest grocery retailers to streaming processing for transactional data. By leveraging Apache Beam's powerful transforms and robust streaming capabilities, they achieved a 5x reduction in infrastructure costs and a 4x boost in performance. The streaming Apache Beam pipelines now process over 100 million rows daily, consolidating hundreds of gigabytes of transactional data with over a terabyte of an external state in under 3 hours, a task that was not feasible without Apache Beam’s controlled aggregation."
authorName: "OCTO Technology's Data Engineering Team"
authorPosition: "Large Retail Client Project"
authorImg: /images/logos/powered-by/octo.png
publishDate: 2023-08-10T00:12:00+00:00
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
        <img src="/images/logos/powered-by/octo.png"/>
    </div>
    <blockquote class="case-study-quote-block">
      <p class="case-study-quote-text">
        “Oftentimes, I tell the clients that Apache Beam is the “Docker” of data processing. It's highly portable, runs seamlessly anywhere, unifies batch and streaming processing, and offers numerous out-of-the-box templates. Adopting Beam enables accelerated migration from batch to streaming, effortless pipeline reuse across contexts, and faster enablement of new use cases. The benefits and great performance of Apache Beam are a game-changer for many!”
      </p>
      <div class="case-study-quote-author">
        <div class="case-study-quote-author-img">
            <img src="/images/case-study/octo/godefroy-clair.png">
        </div>
        <div class="case-study-quote-author-info">
            <div class="case-study-quote-author-name">
              Godefroy Clair
            </div>
            <div class="case-study-quote-author-position">
              Data Architect @ OCTO Technology
            </div>
        </div>
      </div>
    </blockquote>
</div>
<div class="case-study-post">

# High-Performing and Efficient Transactional Data Processing for OCTO Technology’s Clients

## Background

[OCTO Technology](https://octo.com/), part of [Accenture](https://www.accenture.com/), stands at the forefront of technology consultancy and software engineering, specializing in new technologies and digital transformation. Since 1998, OCTO has been dedicated to crafting scalable digital solutions that drive business transformations for clients, ranging from startups to multinational corporations. OCTO leverages its deep technology expertise and a strong culture of successful innovation to help clients explore, test, and embrace emerging technologies or implement mature digital solutions at scale.

With the powerful Apache Beam unified portable model, OCTO has unlocked the potential to empower, transform, and scale the data ecosystems of several clients, including renowned names like a famous French newspaper and one of France's largest grocery retailers.


In this spotlight, OCTO’s Data Architect, Godefroy Clair, and Data Engineers, Florian Bastin and Leo Babonnaud, unveil the remarkable impact of Apache Beam on the data processing of a leading French grocery retailer. The implementation led to expedited migration from batch to streaming, a 4x acceleration in transactional data processing, and a 5x improvement in infrastructure cost efficiency.


## High-performing transactional data processing

OCTO’s Client, a prominent grocery and convenience store retailer with tens of thousands of stores across several countries, relies on an internal web app to empower store managers with informed purchasing decisions and effective store management. The web app provides access to crucial product details, stock quantities, pricing, promotions, and more, sourced from various internal data stores, platforms, and systems.

Before 2022, the Client utilized an orchestration engine for orchestrating batch pipelines that consolidated and processed data from Cloud Storage files and Pub/Sub messages and wrote the output to BigQuery. However, with most source data uploaded at night, batch processing posed challenges in meeting SLAs and providing the most recent information to store managers before store opening. Moreover, incorrect or missing data uploads required cumbersome database state reverts, involving a substantial amount of transactional data and logs. The Client’s internal team dedicated significant time to maintaining massive SQL queries or manually updating the database state, resulting in high maintenance costs.

To address these issues, the Client sought OCTO's expertise to transform their data ecosystem and migrate their core use case from batch to streaming. The objectives included faster data processing, ensuring the freshest data in the web app, simplifying pipeline and database maintenance, ensuring scalability and resilience, and efficiently handling spikes in data volumes.

<blockquote class="case-study-quote-block case-study-quote-wrapped">
  <p class="case-study-quote-text">
    The Client needed to very quickly consolidate and process a huge number of files in different formats from Cloud Storage and Pub/Sub events to have the freshest info about new products, promotions, etc. in their web app every day. For all that, Apache Beam was the perfect tool.
  </p>
  <div class="case-study-quote-author">
    <div class="case-study-quote-author-img">
        <img src="/images/case-study/octo/godefroy-clair.png">
    </div>
    <div class="case-study-quote-author-info">
        <div class="case-study-quote-author-name">
          Godefroy Clair
        </div>
        <div class="case-study-quote-author-position">
          Data Architect @ OCTO Technology
        </div>
    </div>
  </div>
</blockquote>

Apache Beam and its unified model emerged as the perfect solution, enabling both near-real-time streaming for the Client’s core transactional data processing, as well as their batch processing for standalone use cases. Additionally, it offered the added benefit of autoscaling with the [Dataflow runner](/documentation/runners/dataflow/). With Apache Beam's [ Python SDK](/documentation/sdks/python/) and the out-of-the-box [I/O connectors](/documentation/io/connectors/), OCTO was able to reuse Python components between the existing and new batch and streaming pipelines, and leverage native optimized connectivity with Pub/Sub and Cloud Storage, expediting the migration.

<div class="post-scheme">
    <a href="/images/case-study/octo/scheme-14.png" target="_blank" title="Click to enlarge">
        <img src="/images/case-study/octo/scheme-14.png" alt="streaming pipelines">
    </a>
</div>

The streaming Apache Beam pipeline behind the Client’s web app now processes product and inventory data from Pub/Sub messages and Cloud Storage files of varying sizes - from several rows to 1.7 million rows - that arrive in Cloud Storage at various times, in unpredictable order, and in various formats (such as CSV, JSON, and zip files). Apache Beam's [timely processing](/blog/timely-processing/) capabilities enable the streaming pipeline to handle that data efficiently. Its [timers](/documentation/basics/#state-and-timers) provide a way to control aggregations by waiting until all the necessary events and files come in and then processing them in the right order, while the [GroupByKey](/documentation/transforms/python/aggregation/groupbykey/) and [GroupIntoBatches](/documentation/transforms/python/aggregation/groupintobatches/) transforms allow for efficient grouping for every key and batching the input into desired size. Every day, the Apache Beam pipeline consolidates, deduplicates, enriches, and outputs the data to [Firestore](https://firebase.google.com/docs/firestore) and [Algolia](https://www.algolia.com/), processing over 100 million rows and consolidating hundreds of gigabytes of transactional data with over a terabyte of an external state within less than 3 hours.

<blockquote class="case-study-quote-block case-study-quote-wrapped">
  <p class="case-study-quote-text">
    The web app requires fresh data early in the morning before the stores open. Previously, handling the entirety of the Client’s data in time was not feasible. Thanks to Apache Beam, they can now process it within just 3 hours, ensuring data availability even if the input files arrive late at night.
  </p>
  <div class="case-study-quote-author">
    <div class="case-study-quote-author-img">
        <img src="/images/case-study/octo/leo-babonnaud.jpg">
    </div>
    <div class="case-study-quote-author-info">
        <div class="case-study-quote-author-name">
          Leo Babonnaud
        </div>
        <div class="case-study-quote-author-position">
          Data Scientist @ OCTO Technology
        </div>
    </div>
  </div>
</blockquote>

The Client’s specific use case posed unique challenges: the enrichment data was too large to keep in memory, and the unpredictable file order and arrival rendered timers and state API unfeasible. Being unable to leverage Apache Beam's native stateful processing, OCTO found a solution in externalizing the state of [DoFns](/documentation/programming-guide/#pardo) to a transactional [Cloud SQL](https://cloud.google.com/sql/docs/introduction) Postgres database. When processing new events and files, the Apache Beam pipeline uses streaming queries to select, insert, upsert, and delete rows with states in the Cloud SQL database. Apache Beam excels in complex state consolidation when processing files, Pub/Sub events, and logs representing the past, present, and future state of the records in the sink databases. If the incoming data is wrong and the sink data stores need to be reverted, Apache Beam processes a huge amount of logs about data movements that happened within a specific timeframe and consolidates them into states, eliminating manual efforts.

<blockquote class="case-study-quote-block case-study-quote-wrapped">
  <p class="case-study-quote-text">
    The web app requires fresh data early in the morning before the stores open. Previously, handling the entirety of the Client’s data in time was not feasible. Thanks to Apache Beam, they can now process it within just 3 hours, ensuring data availability even if the input files arrive late at night.
  </p>
  <div class="case-study-quote-author">
    <div class="case-study-quote-author-img">
        <img src="/images/case-study/octo/florian-bastin.jpg">
    </div>
    <div class="case-study-quote-author-info">
        <div class="case-study-quote-author-name">
          Florian Bastin
        </div>
        <div class="case-study-quote-author-position">
          Lead Data Scientist @ OCTO Technology
        </div>
    </div>
  </div>
</blockquote>

By leveraging Apache Beam, the Client has achieved a groundbreaking transformation in data processing, empowering their internal web app with fresh and historical data, enhancing overall operational efficiency, and meeting business requirements with improved processing latency.

## Custom I/O and fine-grained control over SQL connections

The Client’s specific use case demanded CRUD operations in a Cloud SQL database based on a value in a PCollection, and although the built-in [JBDC I/O connector](https://beam.apache.org/releases/pydoc/current/apache_beam.io.jdbc.html) supported reading and writing from a Cloud SQL database, it did not cater to such SQL operations. However, Apache Beam's custom I/O frameworks open the door for [creating new connectors](/documentation/io/developing-io-overview/) tailored to complex use cases, offering the same connectivity as out-of-the-box I/Os. Capitalizing on this advantage and leveraging [ParDo](/documentation/transforms/python/elementwise/pardo/) and [GroupByKey](/documentation/transforms/python/aggregation/groupbykey/) transforms, OCTO successfully developed a new Apache Beam I/O. This custom I/O seamlessly interacts with a Cloud SQL database using the [Cloud SQL Python Connector](https://pypi.org/project/cloud-sql-python-connector/), instantiating the latter as a connection object in the [DoFn.Setup](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html#apache_beam.transforms.core.DoFn.setup) method.

Moreover, Apache Beam offered OCTO fine-grained control over parallelism, enabling them to maximize worker processes' efficiency. With the Dataflow runner's potent parallelism and autoscaling capabilities, OCTO had to address the [constraints on the number of concurrent connections](https://cloud.google.com/sql/docs/quotas) imposed by Cloud SQL. To overcome this challenge, the Apache Beam DoFn.Setup method came into play, providing a means to define the maximum number of concurrent operations by specifying it within the method. OCTO also leveraged the [beam.utils.Shared](https://beam.apache.org/releases/pydoc/2.29.0/apache_beam.utils.shared.html) module to create a connection pool for the Cloud SQL database, effectively sharing it across all processes at the worker level.

OCTO's data engineers showcased these innovative developments powered by Apache Beam at [Beam Summit 2023](https://beamsummit.org/sessions/2023/how-to-balance-power-and-control-when-using-dataflow-with-an-oltp-sql-database/).

## Results

Apache Beam enabled OCTO to revolutionize data processing of one of the most prominent French grocery retailers with a 5x optimization in infrastructure costs and a 4x increase in data processing performance. Apache Beam's unified model and Python SDK proved instrumental in accelerating the migration from batch to streaming processing by providing the ability to reuse components, packages, and modules across pipelines.

Apache Beam's powerful transforms and robust streaming capabilities enabled the Client’s streaming pipeline to efficiently process over 100 million rows daily, consolidating transactional data with over a terabyte of an external state in under 3 hours, a feat that was previously unattainable. The flexibility and extensibility of Apache Beam empowered OCTO to tackle use-case-specific technical constraints, achieving the perfect balance of power and control to align with the Client’s specific business objectives.

<blockquote class="case-study-quote-block case-study-quote-wrapped">
  <p class="case-study-quote-text">
    Oftentimes, I tell the clients that Apache Beam is the “Docker” of data processing. It's highly portable, runs seamlessly anywhere, unifies batch and streaming processing, and offers numerous out-of-the-box templates. Adopting Beam enables accelerated migration from batch to streaming, effortless pipeline reuse across contexts, and faster enablement of new use cases. The benefits and great performance of Apache Beam are a game-changer for many!
  </p>
  <div class="case-study-quote-author">
    <div class="case-study-quote-author-img">
        <img src="/images/case-study/octo/godefroy-clair.png">
    </div>
    <div class="case-study-quote-author-info">
        <div class="case-study-quote-author-name">
          Godefroy Clair
        </div>
        <div class="case-study-quote-author-position">
          Data Architect @ OCTO Technology
        </div>
    </div>
  </div>
</blockquote>

## Learn More

<iframe class="video video--medium-size" width="560" height="315" src="https://www.youtube.com/embed/TueDlUBJsQU" frameborder="0" allowfullscreen></iframe>
<br><br>

{{< case_study_feedback "OCTO" >}}

</div>
<div class="clear-nav"></div>
