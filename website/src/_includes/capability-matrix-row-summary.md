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
<center><b>{% if val.l1 == 'Yes' %}&#x2713;{% elsif val.l1 == 'Partially' %}~{% else %}&#x2715;{% endif %}{% if val.jira %} (<a href='https://issues.apache.org/jira/browse/{{ val.jira }}'>{{ val.jira }}</a>){% endif %}</b></center>
