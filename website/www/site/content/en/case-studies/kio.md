---
title:  "Kio"
icon: /images/logos/powered-by/kio.png
hasNav: true
cardDescription: "Kio is a set of Kotlin extensions for Apache Beam to implement fluent-like API for Java SDK."
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

<div class="case-study-post">

# Kio is a set of Kotlin extensions for Apache Beam to implement fluent-like API for Java SDK.<!--more-->

## Word Count example

```
// Create Kio context
val kio = Kio.fromArguments(args)

// Configure a pipeline
kio.read().text("~/input.txt")
    .map { it.toLowerCase() }
    .flatMap { it.split("\\W+".toRegex()) }
    .filter { it.isNotEmpty() }
    .countByValue()
    .forEach { println(it) }

// And execute it
kio.execute().waitUntilDone()
```

## Documentation

For more information about Kio, please see the documentation here: [https://code.chermenin.ru/kio](https://code.chermenin.ru/kio).
</div>
<div class="clear-nav"></div>
