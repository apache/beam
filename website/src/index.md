---
layout: default
body_class: body--index

logos:
- title: APEX
  image_url: /images/logo_apex.png
  url: "http://apex.apache.org"
- title: Flink
  image_url: /images/logo_flink.png
  url: "http://flink.apache.org"
- title: Spark
  image_url: /images/logo_spark.png
  url: http://spark.apache.org/
- title: Google Cloud Dataflow
  image_url: /images/logo_google_cloud.png
  url: https://cloud.google.com/dataflow/
- title: Gearpump
  image_url: /images/logo_gearpump.png
  url: http://gearpump.apache.org/
- title: Samza
  image_url: /images/logo_samza.png
  url: http://samza.apache.org/

pillars:
- title: Unified
  body: Use a single programming model for both batch and streaming use cases.
- title: Portable
  body: Execute pipelines on multiple execution environments.
- title: Extensible
  body: Write and share new SDKs, IO connectors, and transformation libraries.

cards:
- quote: "A framework that delivers the flexibility and advanced functionality our customers need."
  name: –Talend
- quote: "Apache Beam has powerful semantics that solve real-world challenges of stream processing."
  name: –PayPal
- quote: "Apache Beam represents a principled approach for analyzing data streams."
  name: –data Artisans
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
<div class="hero-bg">
  <div class="hero section">
    <div class="hero__cols">
      <div class="hero__cols__col">
        <div class="hero__cols__col__content">
          <div class="hero__title">
            Apache Beam: An advanced unified programming model
          </div>
          <div class="hero__subtitle">
            Implement batch and streaming data processing jobs that run on any execution engine.
          </div>
          <div class="hero__ctas hero__ctas--first">
            <a class="button button--primary" href="{{'/get-started/beam-overview/'|prepend:site.baseurl}}">Learn more</a>
            <a class="button button--primary" href="{{'/get-started/try-apache-beam/'|prepend:site.baseurl}}">Try Beam</a>
            <a class="button button--primary" href="{{'/get-started/downloads/'|prepend:site.baseurl}}">Download Beam SDK {{ site.release_latest }}</a>
          </div>
          <div class="hero__ctas">
            <a class="button" href="{{'/get-started/quickstart-java/'|prepend:site.baseurl}}">Java Quickstart</a>
            <a class="button" href="{{'/get-started/quickstart-py/'|prepend:site.baseurl}}">Python Quickstart</a>
            <a class="button" href="{{'/get-started/quickstart-go/'|prepend:site.baseurl}}">Go Quickstart</a>
          </div>
        </div>
      </div>
      <div class="hero__cols__col">
        <div class="hero__blog">
          <div class="hero__blog__title">
            The latest from the blog
          </div>
          <div class="hero__blog__cards">
            {% for post in site.posts limit:3 %}
            <a class="hero__blog__cards__card" href="{{ post.url | prepend: site.baseurl }}">
              <div class="hero__blog__cards__card__title">{{post.title}}</div>
              <div class="hero__blog__cards__card__date">{{ post.date | date: "%b %-d, %Y" }}</div>
            </a>
            {% endfor %}
          </div>
        </div>
      </div>
    </div>
  </div>
</div>

<div class="pillars section">
  <div class="pillars__title">
    All about Apache Beam
  </div>
  <div class="pillars__cols">
    {% for pillar in page.pillars %}
    <div class="pillars__cols__col">
      <div class="pillars__cols__col__title">
        {{pillar.title}}
      </div>
      <div class="pillars__cols__col__body">
        {{pillar.body}}
      </div>
    </div>
    {% endfor %}
  </div>
</div>

<div class="graphic section">
<div class="graphic__image">
<img src="{{ '/images/beam_architecture.png' | prepend: site.baseurl }}" alt="Beam architecture">
</div>
</div>

<div class="logos section">
  <div class="logos__title">
    Works with
  </div>
  <div class="logos__logos">
    {% for logo in page.logos %}
    <div class="logos__logos__logo">
      <a href="{{ logo.url | prepend: base.siteUrl }}"><img src="{{logo.image_url|prepend:site.baseurl}}" alt="{{logo.title}}"></a>
    </div>
    {% endfor %}
  </div>
</div>

<div class="cards section section--wide">
  <div class="section__contained">
    <div class="cards__title">
      Testimonials
    </div>
    <div class="cards__cards">
      {% for card in page.cards %}
      <div class="cards__cards__card">
        <div class="cards__cards__card__body">
          {{card.quote}}
        </div>
        <div class="cards__cards__card__user">
          <!-- TODO: Implement icons.
          <div class="cards__cards__card__user__icon">
          </div>
          -->
          <div class="cards__cards__card__user__name">
            {{card.name}}
          </div>
        </div>
      </div>
      {% endfor %}
    </div>
    <div class="cards__body">
      Beam is an open source community and contributions are greatly appreciated!
      If you’d like to contribute, please see the <a href="{{'/contribute/'|prepend:site.baseurl}}">Contribute</a> section.
    </div>
  </div>
</div>

<div class="ctas section">
  <div class="ctas__title">
    Get started
  </div>
  <div class="ctas__ctas ctas__ctas--top">
  <a class="button button--primary" href="{{'/get-started/beam-overview/'|prepend:site.baseurl}}">Learn more</a>
  <a class="button button--primary" href="{{'/get-started/downloads/'|prepend:site.baseurl}}">Download Beam SDK {{ site.release_latest }}</a>
  </div>
  <div class="ctas__ctas">
  <a class="button" href="{{'/get-started/quickstart-java/'|prepend:site.baseurl}}">Java Quickstart</a>
  <a class="button" href="{{'/get-started/quickstart-py/'|prepend:site.baseurl}}">Python Quickstart</a>
  <a class="button" href="{{'/get-started/quickstart-go/'|prepend:site.baseurl}}">Go Quickstart</a>
  </div>
</div>
