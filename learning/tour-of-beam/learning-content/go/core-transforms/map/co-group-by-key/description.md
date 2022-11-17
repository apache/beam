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

# CoGroupByKey

You can use `CoGroupByKey` relational union of two or more collections of keys/values that have the same key type.

`CoGroupByKey` accepts an arbitrary number of `PCollections` as input. As output, `CoGroupByKey` creates a single output `PCollection` that groups each key with value iterator functions for each input `PCollection`. The iterator functions map to input `PCollections` in the same order they were provided to the `CoGroupByKey`.

```
type stringPair struct {
	K, V string
}

func splitStringPair(e stringPair) (string, string) {
	return e.K, e.V
}

// CreateAndSplit is a helper function that creates
func CreateAndSplit(s beam.Scope, input []stringPair) beam.PCollection {
	initial := beam.CreateList(s, input)
	return beam.ParDo(s, splitStringPair, initial)
}

var emailSlice = []stringPair{
	{"amy", "amy@example.com"},
	{"carl", "carl@example.com"},
	{"julia", "julia@example.com"},
	{"carl", "carl@email.com"},
}

var phoneSlice = []stringPair{
	{"amy", "111-222-3333"},
	{"james", "222-333-4444"},
	{"amy", "333-444-5555"},
	{"carl", "444-555-6666"},
}

emails := CreateAndSplit(s.Scope("CreateEmails"), emailSlice)
phones := CreateAndSplit(s.Scope("CreatePhones"), phoneSlice)

results := beam.CoGroupByKey(s, emails, phones)

contactLines := beam.ParDo(s, formatCoGBKResults, results)


func formatCoGBKResults(key string, emailIter, phoneIter func(*string) bool) string {
	var s string
	var emails, phones []string
	for emailIter(&s) {
		emails = append(emails, s)
	}
	for phoneIter(&s) {
		phones = append(phones, s)
	}
	// Values have no guaranteed order, sort for deterministic output.
	sort.Strings(emails)
	sort.Strings(phones)
	return fmt.Sprintf("%s; %s; %s", key, formatStringIter(emails), formatStringIter(phones))
}

```

The following code example joins the two `PCollection`s with `CoGroupByKey`, followed by a `ParDo` to consume the result. Then, the code uses tags to look up and format data from each collection.


### Playground exercise

You can find the full code of this example in the playground window, which you can run and experiment with.

In the code, we combined the data using the first letters of fruits and the first letters of countries. And the result was like this: `(Alphabet) key: first letter, (Country) values-1:Country, (Fruit) values-2:Fruits`

You can work if you have ready-made kv data, for example, you want to combine using countries. And output the weight of the fruit:
```
weight := beam.ParDo(s, func(_ []byte, emit func(string, int)){
		emit("brazil", 1000)
		emit("australia", 150)
		emit("canada", 340)
}, beam.Impulse(s))

fruits := beam.ParDo(s, func(_ []byte, emit func(string, string)){
		emit("australia", "cherry")
		emit("brazil", "apple")
		emit("canada", "banan")
}, beam.Impulse(s))
```

Change `Alphabet` to `ProductWeight`:
```
type WordsAlphabet struct {
	Country string
	Fruit string
	ProductWeight int
}
```

The union takes place through the keys:
```
func applyTransform(s beam.Scope, fruits beam.PCollection, countries beam.PCollection) beam.PCollection {
	grouped := beam.CoGroupByKey(s, fruits, countries)
	return beam.ParDo(s, func(key string, weightIter func(*int) bool, fruitIter func(*string) bool, emit func(string)) {

	wa := &WordsAlphabet{
		Country: key,
	}
	weightIter(&wa.ProductWeight)
	fruitIter(&wa.Fruit)
    emit(wa.String())
	
	}, grouped)
}
```

Have you also noticed the order in which the collection items are displayed in the console? Why is that? You can also run the example several times to see if the output remains the same or changes.