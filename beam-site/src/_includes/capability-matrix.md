<div id='cap-{{ cap-view }}' style='display:{{ cap-display }}'>
<table class='{{ cap-style }}'>
  {% for category in cap-data.categories %}
  <tr class='{{ cap-style }}' id='cap-{{ cap-view }}-{{ category.anchor }}'>
    <th class='{{ cap-style }} color-metadata format-category' colspan='5' style='color:#{{ category.color-b }}'>{% if cap-view != 'blog' %}<div class='cap-toggle' onclick='ToggleTables({{ cap-toggle-details }}, "cap-{{ cap-other-view }}-{{ category.anchor }}")'>({% if cap-toggle-details == 1 %}expand{% else %}collapse{% endif %} details)</div>{% endif %}{{ category.description }}</th>
  </tr>
  <tr class='{{ cap-style }}'>
    <th class='{{ cap-style }} color-capability'></th>
  {% for x in cap-data.columns %}
    <th class='{{ cap-style }} color-platform format-platform' style='color:#{{ category.color-y }}'>{{ x.name }}</th>
  {% endfor %}
  </tr>
  {% for row in category.rows %}
  <tr class='{{ cap-style }}'>
    <th class='{{ cap-style }} color-capability format-capability' style='color:#{{ category.color-y }}'>{{ row.name }}</th>
    {% for val in row.values %}
    {% capture value-markdown %}{% include capability-matrix-row-{{ cap-view }}.md %}{% endcapture %}

    <td width='25%' class='{{ cap-style }}' style='background-color:#{% if val.l1 == 'Yes' %}{{ category.color-y }}{% elsif val.l1 == 'Partially' %}{{ category.color-p }}{% else %}{{ category.color-n }}{% endif %};border-color:#{{category.color-b}}'>{{ value-markdown }}</td>
    {% endfor %}
  </tr>
  {% endfor %}
  <tr class='{{ cap-style }}'>
    <td class='{{ cap-style }} color-blank cap-blank' colspan='5'></td>
  </tr>
  {% endfor %}
</table>
</div>
