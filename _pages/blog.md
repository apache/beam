---
layout: page
title: "Apache Beam Blog"
permalink: /blog/
---

This is the blog for the Apache Beam project. This blog contains news and updates
for the project.

{% for post in site.posts %}
{% assign author = site.data.authors[post.author] %}

### <a class="post-link" href="{{ post.url | prepend: site.baseurl }}">{{ post.title }}</a>
<i>{{ post.date | date: "%b %-d, %Y" }}{% if author %} - posted by {{ author.name }} [<a href="https://twitter.com/{{ author.twitter }}">@{{ author.twitter }}</a>]
{% endif %}</i>

{{ post.excerpt }}

<hr>
{% endfor %}
