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
# Side inputs
In addition to the main input `PCollection`, you can provide additional inputs to a `ParDo` transform in the form of side inputs. A side input is an additional input that your DoFn can access each time it processes an element in the input PCollection. When you specify a side input, you create a view of some other data that can be read from within the ParDo transform’s DoFn while processing each element.

Side inputs are useful if your `ParDo` needs to inject additional data when processing each element in the input PCollection, but the additional data needs to be determined at runtime (and not hard-coded). Such values might be determined by the input data, or depend on a different branch of your pipeline.

### Passing side inputs to ParDo

```
  // Pass side inputs to your ParDo transform by invoking .withSideInputs.
  // Inside your DoFn, access the side input by using the method DoFn.ProcessContext.sideInput.

  // The input PCollection to ParDo.
  PCollection<String> words = ...;

  // A PCollection of word lengths that we'll combine into a single value.
  PCollection<Integer> wordLengths = ...; // Singleton PCollection

  // Create a singleton PCollectionView from wordLengths using Combine.globally and View.asSingleton.
  final PCollectionView<Integer> maxWordLengthCutOffView =
     wordLengths.apply(Combine.globally(new Max.MaxIntFn()).asSingletonView());


  // Apply a ParDo that takes maxWordLengthCutOffView as a side input.
  PCollection<String> wordsBelowCutOff =
  words.apply(ParDo
      .of(new DoFn<String, String>() {
          @ProcessElement
          public void processElement(@Element String word, OutputReceiver<String> out, ProcessContext c) {
            // In our DoFn, access the side input.
            int lengthCutOff = c.sideInput(maxWordLengthCutOffView);
            if (word.length() <= lengthCutOff) {
              out.output(word);
            }
          }
      }).withSideInputs(maxWordLengthCutOffView)
  );
```

### Playground exercise

You can find the full code of this example in the playground window, which you can run and experiment with.

At the entrance we have a map whose key is the city of the country value. And we also have a `Person` structure with his name and city. We can compare cities and embed countries in `Person`.

You can also use it as a variable for mathematical calculations.

Before you start, add a dependency:
```
"fmt"
"time"
```

Changing `citiesToCountriesKV` to `citiesToTimeKV`:
```
citiesToTimeKV := beam.ParDo(s, func(_ []byte, emit func(string, int)){
		emit("Beijing", 8)
		emit("London", 0)
		emit("San Francisco", -8)
		emit("Singapore", 8)
		emit("Sydney", 11)
}, beam.Impulse(s))
```

Calculate the current time and add GMT:
```
func joinFn(person Person, citiesToCountriesIter func(*string,*int) bool, emit func(Person)) {
var city string
var gmt int
now := time.Now()

for citiesToCountriesIter(&city,&gmt) {
    time := now.Hour()+gmt

	if person.City == city {
        if time < 0 {
            time = 24 + (now.Hour() + gmt)
        }
		emit(Person{
		    Name:    person.Name,
			City:    city,
            Time: (fmt.Sprintf("%d:%d",time,now.Minute())),
		})
		break
		}
	}
}
```

Have you also noticed the order in which the collection items are displayed in the console? Why is that? You can also run the example several times to see if the output remains the same or changes.