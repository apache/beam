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
# FlatMap elements

It works like `Map elements` , but inside the logic you can do complex operations like dividing the list into separate elements and processing

```
with beam.Pipeline() as p:

    (p | beam.Create(['Apache Beam', 'Unified Batch and Streaming'])
     | beam.FlatMap(lambda sentence: sentence.split())
     | LogElements())
```

### Description for example

At the input, the elements of the `PCollection` are represented as strings. The `beam.FlatMap()` returns a list of words that make up the sentence.