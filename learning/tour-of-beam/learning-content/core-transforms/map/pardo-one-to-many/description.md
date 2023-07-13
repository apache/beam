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

{{if (eq .Sdk "go")}}
```
func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
	return beam.ParDo(s, tokenizeFn, input)
}

func tokenizeFn(input string, emit func(out string)) {
	tokens := strings.Split(input, " ")
	for _, k := range tokens {
		emit(k)
	}
}
```
{{end}}
{{if (eq .Sdk "java")}}
```
static PCollection<String> applyTransform(PCollection<String> input) {
        return input.apply(ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(@Element String sentence, OutputReceiver<String> out) {
                // Divided sentences into words
                String[] words = sentence.split(" ");

                // Operations on each element
                for (String word : words) {
                    out.output(word);
                }
            }

        }));
    }
```
{{end}}

{{if (eq .Sdk "python")}}
```
class BreakIntoWordsDoFn(beam.DoFn):
    def process(self, element):
        return element.split()

with beam.Pipeline() as p:
  (p | beam.Create(['Hello Beam', 'It is awesome'])
     | beam.ParDo(BreakIntoWordsDoFn())
     | LogElements())
```
{{end}}
### Playground exercise

You can find the full code of this example in the playground window, which you can run and experiment with.

You can nest other logical operations.