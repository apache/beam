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

{% capture notebook_url %}https://colab.research.google.com/github/{{ site.branch_repo }}/{{ include.notebook }}{% endcapture %}

{% capture code_url %}https://github.com/{{ site.branch_repo }}/{{ include.code }}{% endcapture %}

{:.notebook-skip}
{% include button.md
  url=notebook_url
  logo="https://github.com/googlecolab/open_in_colab/raw/master/images/icon32.png"
  text="Run code now"
%}

{% include button.md
  url=code_url
  logo="https://www.tensorflow.org/images/GitHub-Mark-32px.png"
  text="View source code"
%}

<br><br><br>
