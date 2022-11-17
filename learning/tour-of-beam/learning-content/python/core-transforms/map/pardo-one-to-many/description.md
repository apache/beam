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
# ParDo one-to-many

It works like `ParDo` one-to-one, but inside the logic you can do complex operations like dividing the list into separate elements and processing

```
class BreakIntoWordsDoFn(beam.DoFn):

    def process(self, element):
        return element.split()
        
with beam.Pipeline() as p:

  (p | beam.Create(['Hello Beam', 'It is awesome'])
     | beam.ParDo(BreakIntoWordsDoFn())
     | LogElements())
```

### Description for example 

At the input, the elements of the "Collection" are represented as strings. The `applyTransform()` function returns a list of words that make up a sentence.