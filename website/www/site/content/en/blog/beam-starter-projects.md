---
title:  "Beam starter projects"
date:   2022-11-03 9:00:00 -0700
categories:
- blog
authors:
- davidcavazos
---
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

We're happy to announce that we're providing new Beam starter projects! 🎉

Setting up and configuring a new project can be time consuming, and varies in different languages. We hope this will make it easier for you to get started in creating new Apache Beam projects and pipelines.

<!--more-->

All the starter projects come in their own GitHub repository, so you can simply clone a repo and you're ready to go. Each project comes with a README with how to use it, a simple "Hello World" pipeline, and a test for the pipeline. The GitHub repositories come pre-configured with GitHub Actions to automatically run tests when pull requests are opened or modified, and Dependabot is enabled to make sure all the dependencies are up to date. This all comes out of the box, so you can start playing with your Beam pipeline without a hassle.

For example, here's how to get started with Java:

```
git clone https://github.com/apache/beam-starter-java
cd beam-starter-java

# Install Java and Gradle with sdkman.
curl -s "https://get.sdkman.io" | bash
sdk install java 11.0.12-tem
sdk install gradle

# To run the pipeline.
gradle run

# To run the tests.
gradle test
```

And here's how to get started with Python:

```
git clone https://github.com/apache/beam-starter-python
cd beam-starter-python

# Set up a virtual environment with the dependencies.
python -m venv env
source env/bin/activate
pip install -r requirements.txt

# To run the pipeline.
python main.py

# To run the tests.
python -m unittest
```

Here are the starter projects; you can choose your favorite language:

* **[Java]** [github.com/apache/beam-starter-java](https://github.com/apache/beam-starter-java) – Includes both Gradle and Maven configurations.
* **[Python]** [github.com/apache/beam-starter-python](https://github.com/apache/beam-starter-python) – Includes a setup.py file to allow multiple files in your pipeline.
* **[Go]** [github.com/apache/beam-starter-go](https://github.com/apache/beam-starter-go) – Includes how to register different types of functions for ParDo.
* **[Kotlin]** [github.com/apache/beam-starter-kotlin](https://github.com/apache/beam-starter-kotlin) – Adapted to idiomatic Kotlin
* **[Scala]** [github.com/apache/beam-starter-scala](https://github.com/apache/beam-starter-scala) – Coming soon!

We have updated the [Java quickstart](/get-started/quickstart/java/) to use the new starter project, and we're working on updating the Python and Go quickstarts as well.

We hope you find this useful. Feedback and contributions are always welcome! So feel free to create a GitHub issue, or open a Pull Request to any of the starter project repositories.
