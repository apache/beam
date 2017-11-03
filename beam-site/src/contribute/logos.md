---
layout: section
title: "Beam Logos"
section_menu: section-menu/contribute.html
permalink: /contribute/logos/
redirect_from:
  - /project/logos/
  - /material/
---

# Apache Beam Logos

This page contains project material for the Apache Beam project.

## Project logos
You can download [this archive]({{ site.baseurl }}/{{ site.downloads }}/{{ site.data.logos.archive-file }})
containing all of the logos or download the logos individually.

### Scalable Vector Graphics (SVG)
These [SVG files](https://en.wikipedia.org/wiki/Scalable_Vector_Graphics) can
be resized easily and are suitable for print or web use. Click on the logo to
download it.

{% for color in site.data.logos.colors %}
#### {{ color[1] }}
<div class="row">
<div class="col-md-2">
</div>
{% for type in site.data.logos.types %}
<div class="col-md-2">
<div class="row">
<a href="{{ site.baseurl }}{{ site.data.logos.logo-location }}/{{ color[0] }}/{{ type }}/beam-logo-{{ color[0] }}-{{ type }}.svg" role="button"><img style="height: 60px" src="{{ site.baseurl }}{{ site.data.logos.logo-location }}/{{ color[0] }}/{{ type }}/beam-logo-{{ color[0] }}-{{ type }}.svg" alt="beam-logo-{{ color[0] }}-{{ type }}.svg"></a>
</div><br>
</div>
{% endfor %}
</div>
{% endfor %}


### Portable Network Graphics (PNG)
These [PNG files](https://en.wikipedia.org/wiki/Portable_Network_Graphics) are
available in a number of fixed sizes and are optimized for web use.

{% for color in site.data.logos.colors %}
#### {{ color[1] }}
<div class="row">
<div class="col-md-2">
</div>
{% for type in site.data.logos.types %}
<div class="col-md-2">
<div class="row">
<img style="height: 60px" src="{{ site.baseurl }}{{ site.data.logos.logo-location }}/{{ color[0] }}/{{ type }}/beam-logo-{{ color[0] }}-{{ type }}.svg" alt="beam-logo-{{ color[0] }}-{{ type }}">
</div><br>
<div class="row">
{% for size in site.data.logos.sizes %}
<a href="{{ site.baseurl }}{{ site.data.logos.logo-location }}/{{ color[0] }}/{{ type }}/beam-logo-{{ color[0] }}-{{ type }}-{{ size }}.png">{{ size }}x{{ size }}</a>
{% unless forloop.last %},{% endunless %}
{% endfor %}
</div>
</div>
{% endfor %}
</div>
{% endfor %}

## Colors and fonts
The Apache Beam project uses predefined colors and fonts. [This document]({{ site.baseurl }}/{{ site.downloads }}/palette.pdf) has more information.
