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
You can solve the challenge with this way:
1. Complete the compositions function first of all filters all words that begin with "**s**" `ExtractAndCountWord()` in `return (pcoll
   | beam.ParDo(StartWithLetter('s'))
   )`. Then, you can group the words so that they are unique. After that, we return only the key `| beam.combiners.Count.PerElement()
   | beam.Map(lambda kv: kv[0])`.
```
class StartWithLetter(beam.DoFn):
    def __init__(self, letter=''):
        self.letter = letter
    def process(self, element):
        if(element.lower().startswith(self.letter)):
            yield element
```
2. To return two `PCollection` we need to use `ParDo` with nested logic in this case checking for the case of letters:
```
beam.ParDo(ProcessNumbersDoFn()).with_outputs(wordWithLowerCase, main=wordWithUpperCase))
```
ProcessNumbersDoFn realization:
```
class ProcessNumbersDoFn(beam.DoFn):
    def process(self, element):
        if element.startswith('S'):
            yield element
        else:
            yield pvalue.TaggedOutput(wordWithLowerCase, element)
```
3. To check whether words with **lowercase** letters are contained in words with **uppercase** letters, you need to create a `view` using `side-input`.
```
beam.pvalue.AsList(results[wordWithLowerCase])
```

4. After that, you need to check for the presence of:
```
results[wordWithUpperCase] | beam.ParDo(EnrichCountryDoFn(),beam.pvalue.AsList(results[wordWithLowerCase]))
```

Checking the element:
```
class EnrichCountryDoFn(beam.DoFn):
    def process(self, element, wordWithLowerCase):
        if(element.lower() in wordWithLowerCase):
            yield element
```