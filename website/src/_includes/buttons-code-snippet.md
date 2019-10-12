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

{% capture colab_logo %}https://github.com/googlecolab/open_in_colab/raw/master/images/icon32.png{% endcapture %}
{% capture github_logo %}https://www.tensorflow.org/images/GitHub-Mark-32px.png{% endcapture %}

{% capture notebook_url %}https://colab.research.google.com/github/{{ site.branch_repo }}/{{ include.notebook }}{% endcapture %}
{% capture notebook_java %}{{ notebook_url }}-java.ipynb{% endcapture %}
{% capture notebook_py %}{{ notebook_url }}-py.ipynb{% endcapture %}
{% capture notebook_go %}{{ notebook_url }}-go.ipynb{% endcapture %}

{% capture code_url %}https://github.com/{{ site.branch_repo }}{% endcapture %}
{% capture code_java %}{{ code_url }}/{{ include.java }}{% endcapture %}
{% capture code_py %}{{ code_url }}/{{ include.py }}{% endcapture %}
{% capture code_go %}{{ code_url }}/{{ include.go }}{% endcapture %}

{% if include.java %}
{% if include.notebook %}{% include button.md url=notebook_java logo=colab_logo text="Run code now" attrib=".language-java .notebook-skip" %}{% endif %}
{% include button.md url=code_java logo=github_logo text="View source code" attrib=".language-java" %}
{% endif %}

{% if include.py %}
{% if include.notebook %}{% include button.md url=notebook_py logo=colab_logo text="Run code now" attrib=".language-py .notebook-skip" %}{% endif %}
{% include button.md url=code_py logo=github_logo text="View source code" attrib=".language-py" %}
{% endif %}

{% if include.go %}
{% if include.notebook %}{% include button.md url=notebook_go logo=colab_logo text="Run code now" attrib=".language-go .notebook-skip" %}{% endif %}
{% include button.md url=code_go logo=github_logo text="View source code" attrib=".language-go" %}
{% endif %}

<br><br><br>
