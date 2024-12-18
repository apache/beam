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

In addition to the main input `PCollection`, you can provide additional inputs to a `ParDo` transform in the form of side inputs. A side input is an additional input that your `DoFn` can access each time it processes an element in the input `PCollection`. When you specify a side input, you create a view of some other data that can be read from within the `ParDo` transform’s `DoFn` while processing each element.

Side inputs are useful if your `ParDo` needs to inject additional data when processing each element in the input `PCollection`, but the additional data needs to be determined at runtime (and not hard-coded). Such values might be determined by the input data, or depend on a different branch of your pipeline.
{{if (eq .Sdk "go")}}
All side input iterables should be registered using a generic `register.IterX[...]` function. This optimizes runtime execution of the iterable.
{{end}}
### Passing side inputs to ParDo
{{if (eq .Sdk "go")}}
```
// Side inputs are provided using `beam.SideInput` in the DoFn's ProcessElement method.
// Side inputs can be arbitrary PCollections, which can then be iterated over per element
// in a DoFn.
// Side input parameters appear after main input elements, and before any output emitters.
words = ...

// avgWordLength is a PCollection containing a single element, a singleton.
avgWordLength := stats.Mean(s, wordLengths)

// Side inputs are added as with the beam.SideInput option to beam.ParDo.
wordsAboveCutOff := beam.ParDo(s, filterWordsAbove, words, beam.SideInput{Input: avgWordLength})
wordsBelowCutOff := beam.ParDo(s, filterWordsBelow, words, beam.SideInput{Input: avgWordLength})



// filterWordsAbove is a DoFn that takes in a word,
// and a singleton side input iterator as of a length cut off
// and only emits words that are beneath that cut off.
//
// If the iterator has no elements, an error is returned, aborting processing.
func filterWordsAbove(word string, lengthCutOffIter func(*float64) bool, emitAboveCutoff func(string)) error {
	var cutOff float64
	ok := lengthCutOffIter(&cutOff)
	if !ok {
		return fmt.Errorf("no length cutoff provided")
	}
	if float64(len(word)) > cutOff {
		emitAboveCutoff(word)
	}
	return nil
}

// filterWordsBelow is a DoFn that takes in a word,
// and a singleton side input of a length cut off
// and only emits words that are beneath that cut off.
//
// If the side input isn't a singleton, a runtime panic will occur.
func filterWordsBelow(word string, lengthCutOff float64, emitBelowCutoff func(string)) {
	if float64(len(word)) <= lengthCutOff {
		emitBelowCutoff(word)
	}
}

func init() {
	register.Function3x1(filterWordsAbove)
	register.Function3x0(filterWordsBelow)
	// 1 input of type string => Emitter1[string]
	register.Emitter1[string]()
	// 1 input of type float64 => Iter1[float64]
	register.Iter1[float64]()
}

// The Go SDK doesn't support custom ViewFns.
// See https://github.com/apache/beam/issues/18602 for details
// on how to contribute them!
```
{{end}}
{{if (eq .Sdk "java")}}
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
{{end}}
{{if (eq .Sdk "python")}}
```
# Side inputs are available as extra arguments in the DoFn's process method or Map / FlatMap's callable.
# Optional, positional, and keyword arguments are all supported. Deferred arguments are unwrapped into their
# actual values. For example, using pvalue.AsIter(pcoll) at pipeline construction time results in an iterable
# of the actual elements of pcoll being passed into each process invocation. In this example, side inputs are
# passed to a FlatMap transform as extra arguments and consumed by filter_using_length.
words = ...

# Callable takes additional arguments.
def filter_using_length(word, lower_bound, upper_bound=float('inf')):
  if lower_bound <= len(word) <= upper_bound:
    yield word

# Construct a deferred side input.
avg_word_len = (
    words | beam.Map(len)
    | beam.CombineGlobally(beam.combiners.MeanCombineFn()))

# Call with explicit side inputs.
small_words = words | 'small' >> beam.FlatMap(filter_using_length, 0, 3)

# A single deferred side input.
larger_than_average = (
    words | 'large' >> beam.FlatMap(
        filter_using_length, lower_bound=pvalue.AsSingleton(avg_word_len))
)

# Mix and match.
small_but_nontrivial = words | beam.FlatMap(
    filter_using_length,
    lower_bound=2,
    upper_bound=pvalue.AsSingleton(avg_word_len))


# We can also pass side inputs to a ParDo transform, which will get passed to its process method.
# The first two arguments for the process method would be self and element.


class FilterUsingLength(beam.DoFn):
  def process(self, element, lower_bound, upper_bound=float('inf')):
    if lower_bound <= len(element) <= upper_bound:
      yield element

small_words = words | beam.ParDo(FilterUsingLength(), 0, 3)
```
{{end}}
### Side inputs and windowing

A windowed `PCollection` may be infinite and thus cannot be compressed into a single value (or single collection class). When you create a `PCollectionView` of a windowed `PCollection`, the `PCollectionView` represents a single entity per window (one singleton per window, one list per window, etc.).

Beam uses the window(s) for the main input element to look up the appropriate window for the side input element. Beam projects the main input element’s window into the side input’s window set, and then uses the side input from the resulting window. If the main input and side inputs have identical windows, the projection provides the exact corresponding window. However, if the inputs have different windows, Beam uses the projection to choose the most appropriate side input window.

For example, if the main input is windowed using fixed-time windows of one minute, and the side input is windowed using fixed-time windows of one hour, Beam projects the main input window against the side input window set and selects the side input value from the appropriate hour-long side input window.

If the main input element exists in more than one window, then `processElement` gets called multiple times, once for each window. Each call to `processElement` projects the “current” window for the main input element, and thus might provide a different view of the side input each time.

If the side input has multiple trigger firings, Beam uses the value from the latest trigger firing. This is particularly useful if you use a side input with a single global window and specify a trigger.

### Playground exercise

You can find the full code of this example in the playground window, which you can run and experiment with.

At the entrance we have a map whose key is the city of the country value. And we also have a `Person` structure with their name and city. We can compare cities and embed countries in `Person`.

You can also use it as a variable for mathematical calculations.

{{if (eq .Sdk "go")}}
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
{{end}}


{{if (eq .Sdk "java")}}
```
PCollection<KV<String, Integer>> citiesToTimeKV = pipeline
                .apply("ParseCitiesToTimeKV", Create.of(
                        KV.of("Beijing", 8),
                        KV.of("London", 0),
                        KV.of("San Francisco", -8),
                        KV.of("Singapore", 8),
                        KV.of("Sydney", 11)
                ));
```

Calculate the current time and add GMT:
```
PCollection<Person> joined = persons
    .apply("Join", Join.innerJoin(citiesToTimeKV))
    .apply("FormatTime", ParDo.of(new DoFn<KV<String, KV<Person, Integer>>, Person>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
            Person person = c.element().getValue().getKey();
            Integer gmt = c.element().getValue().getValue();
            LocalTime now = LocalTime.now();
            int time = now.getHour() + gmt;
            if(time < 0) {
                time = 24 + (now.getHour() + gmt);
            }
            c.output(new Person(person.name, person.city, String.format("%d:%d", time, now.getMinute())));
        }
    }));

```
{{end}}


{{if (eq .Sdk "python")}}
```
from apache_beam.transforms.util import Create
from apache_beam.transforms.core import ParDo
from apache_beam.transforms.join import CoGroupByKey
from datetime import datetime

pipeline = beam.Pipeline()

persons = p | 'CreatePersons' >> Create([
    {'name': 'John', 'city': 'Beijing'},
    {'name': 'Mary', 'city': 'Singapore'},
    {'name': 'Bob', 'city': 'Sydney'}
])

citiesToTimeKV = p | 'CreateCitiesToTimeKV' >> Create([
    ('Beijing', 8),
    ('London', 0),
    ('San Francisco', -8),
    ('Singapore', 8),
    ('Sydney', 11)
])

def join_fn(persons, cities_to_time):
    person, time = persons[0], cities_to_time[0][1]
    now = datetime.now()
    hour = now.hour + time
    if hour < 0:
        hour = 24 + hour
    return {
        'name': person['name'],
        'city
```
{{end}}
