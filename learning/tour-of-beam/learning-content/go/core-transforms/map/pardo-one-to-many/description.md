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

It works like ParDo one-to-one, but inside the logic you can do complex operations like dividing the list into separate elements and processing

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

### Playground exercise

You can find the full code of this example in the playground window, which you can run and experiment with.

The `applyTransform()` function returns a list of words that make up a sentence.

`ParDo many-to-many` can be used to split a file into words:

Before you start, add a dependency:
```
"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
"regexp"
```

You also need to add a global variable:

```
var (
    wordRE = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)
)
```

Reading data from a file:
```
input := textio.Read(s, "gs://apache-beam-samples/shakespeare/kinglear.txt")

output := applyTransform(s, input)
```

Let's change `applyTransform` to split into words by regexp:
```
func applyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
    return beam.ParDo(s, func(line string, emit func(string)) {
        for _, word := range wordRE.FindAllString(line, -1) {
            emit(word)
        }
    }, input)
}
```