---
title: "ML Dependency Extras"
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

# ML Dependency Extras

In order to make it easy to make sure you are using dependencies which have
been well tested with Beam ML, Beam provides a set of ML extras which can
be installed alongside of Beam. For example, if you want to use a version
of PyTorch which has been tested with Beam, you can install it with:

```
pip install beam[torch]
```

A full set of extras can be found in
[setup.py](https://github.com/apache/beam/blob/6e3cf2b113026e27db7833a1f0fd08977b7c71e1/sdks/python/setup.py#L397).

**Note:** You can also pin to dependencies outside of the extra range with
a normal install - for example:

```
pip install beam==2.XX.0
pip install torch==<version released after Beam 2.XX.0>
```

this will usually work, but can break if the dependency releases a breaking
change between the version Beam tests with and the version you pin to.
