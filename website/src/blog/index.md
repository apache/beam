---
layout: default
title: "Beam Blog"
permalink: /blog/
---

# Apache Beam Blog

This is the blog for the Apache Beam project. This blog contains news and updates
for the project.

{% for post in site.posts %}
{% assign authors = post.authors %}

### <a class="post-link" href="{{ post.url | prepend: site.baseurl }}">{{ post.title }}</a>
<i>{{ post.date | date: "%b %-d, %Y" }}{% if authors %} • {% include authors-list.md %}{% endif %}</i>

{{ post.excerpt }}

<!-- Render a "read more" button if the post is longer than the excerpt -->
{% capture content_words %}
  {{ post.content | number_of_words }}
{% endcapture %}
{% capture excerpt_words %}
  {{ post.excerpt | number_of_words }}
{% endcapture %}
{% if excerpt_words != content_words %}
<p>
<a class="btn btn-default btn-sm" href="{{ post.url | prepend: site.baseurl }}" role="button">
Read more&nbsp;<span class="glyphicon glyphicon-menu-right" aria-hidden="true"></span>
</a>
</p>
{% endif %}

<hr>
{% endfor %}
