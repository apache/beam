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
### Map Elements

If your function is relatively straightforward, you can simplify your use of ```ParDo``` by providing a lightweight DoFn in-line, as a lambda function .

Here’s the previous example, ```ParDo``` with ComputeLengthWordsFn, with the ```DoFn``` specified as a lambda function :

```
# The input PCollection of strings.
words = ...

# Apply a lambda function to the PCollection words.
# Save the result as the PCollection word_lengths.

word_lengths = words | beam.FlatMap(lambda word: [len(word)])
```

If your ```ParDo``` performs a one-to-one mapping of input elements to output elements–that is, for each input element, it applies a function that produces exactly one output element, you can use the higher-level ```Map``` transform.
Here’s the previous example using ```Map```:

```
# The input PCollection of string.
words = ...

# Apply a Map with a lambda function to the PCollection words.
# Save the result as the PCollection word_lengths.

word_lengths = words | beam.Map(len)
```