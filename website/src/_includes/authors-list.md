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
{% assign count = authors | size %}{% for name in authors %}{% if forloop.first == false and count > 2 %},{% endif %}{% if forloop.last and count > 1 %} &amp;{% endif %}{% assign author = site.data.authors[name] %} {{ author.name }} {% if author.twitter %}[<a href="https://twitter.com/{{ author.twitter }}">@{{ author.twitter }}</a>]{% endif %}{% endfor %}
