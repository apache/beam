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
static PCollection<String> applyTransform(PCollection<String> input) {
        return input.apply(
                FlatMapElements.into(TypeDescriptors.strings())
                        .via(sentence -> Arrays.asList(sentence.split(" ")))
        );
    }
```

### Playground exercise

You can find the full code of this example in the playground window, which you can run and experiment with.

You can use other types instead of `Integer`:

```
public static class SumStrings implements SerializableFunction<Iterable<String>, String> {
  @Override
  public String apply(Iterable<String> input) {
    String allWords = 0;
    for (String item : input) {
      allWords += ","+item;
    }
    return allWords;
  }
}
```