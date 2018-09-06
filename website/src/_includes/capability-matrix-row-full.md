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
<center><b>{{ val.l1 }}{% if val.l2 != '' %}: {{ val.l2 }}{% endif %}{% if val.jira %}<br>(<a href='https://issues.apache.org/jira/browse/{{ val.jira }}'>{{ val.jira }}</a>){% endif %}</b></center><br>{{ val.l3 }}
