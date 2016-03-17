---
layout: page
title: "Apache Beam Blog"
permalink: /blog/
---

This is the blog for the Apache Beam project. This blog contains news and updates
for the project.

{% for post in site.posts %}
{% assign authors = post.authors %}

### <a class="post-link" href="{{ post.url | prepend: site.baseurl }}">{{ post.title }}</a>
<i>{{ post.date | date: "%b %-d, %Y" }}{% if authors %} • {% include authors-list.md %}{% endif %}</i>

{{ post.excerpt }}

<hr>
{% endfor %}
