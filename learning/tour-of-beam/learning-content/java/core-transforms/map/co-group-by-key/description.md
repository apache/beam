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

You can use the `CoGroupByKey` transformation for a tuple of tables. `CoGroupByKey` groups results from all tables by similar keys in `CoGbkResults`, from which results for any particular table can be accessed using the `TupleTag` tag supplied with the source table.

For type safety, the Jav SDK requires you to pass each `PCollection` as part of a `KeyedPCollectionTuple`. You must declare a `TupleTag` for each input `PCollection` in the `KeyedPCollectionTuple` that you want to pass to `CoGroupByKey`. As output, `CoGroupByKey` returns a `PCollection<KV<K, CoGbkResult>>`, which groups values from all the input `PCollections` by their common keys. Each key (all of type K) will have a different `CoGbkResult`, which is a map from `TupleTag<T> to Iterable<T>`. You can access a specific collection in an `CoGbkResult` object by using the `TupleTag` that you supplied with the initial collection.

```
// Mock data
final List<KV<String, String>> emailsList =
    Arrays.asList(
        KV.of("amy", "amy@example.com"),
        KV.of("carl", "carl@example.com"),
        KV.of("julia", "julia@example.com"),
        KV.of("carl", "carl@email.com"));

final List<KV<String, String>> phonesList =
    Arrays.asList(
        KV.of("amy", "111-222-3333"),
        KV.of("james", "222-333-4444"),
        KV.of("amy", "333-444-5555"),
        KV.of("carl", "444-555-6666"));

// Creating PCollections
PCollection<KV<String, String>> emails = p.apply("CreateEmails", Create.of(emailsList));
PCollection<KV<String, String>> phones = p.apply("CreatePhones", Create.of(phonesList));

// Create TupleTag for safety type
final TupleTag<String> emailsTag = new TupleTag<>();
final TupleTag<String> phonesTag = new TupleTag<>();

// Apply CoGroupByKey
PCollection<KV<String, CoGbkResult>> results =
    KeyedPCollectionTuple.of(emailsTag, emails)
        .and(phonesTag, phones)
        .apply(CoGroupByKey.create());

// Get result
PCollection<String> contactLines =
    results.apply(
        ParDo.of(
            new DoFn<KV<String, CoGbkResult>, String>() {
              @ProcessElement
              public void processElement(ProcessContext c) {
                KV<String, CoGbkResult> e = c.element();
                String name = e.getKey();
                Iterable<String> emailsIter = e.getValue().getAll(emailsTag);
                Iterable<String> phonesIter = e.getValue().getAll(phonesTag);
                String formattedResult =
                    Snippets.formatCoGbkResults(name, emailsIter, phonesIter);
                c.output(formattedResult);
              }
            }));
```

The following code example joins the two `PCollection`s with `CoGroupByKey`, followed by a `ParDo` to consume the result. Then, the code uses tags to look up and format data from each collection.

### Playground exercise

You can find the full code of this example in the playground window, which you can run and experiment with.

In the code, we combined the data using the first letters of fruits and the first letters of countries. And the result was like this: `(Alphabet) key: first letter, (Country) values-1:Country, (Fruit) values-2:Fruits`

You can work if you have ready-made kv data, for example, you want to combine using countries. And output the weight of the fruit:

```
PCollection<KV<String,Integer>> weightPCollection = pipeline.apply("Countries",
                        Create.of(KV.of("australia", 1000),
                                  KV.of("brazil", 150),
                                  KV.of("canada", 340))
);

PCollection<KV<String,String>> fruitsPCollection = pipeline.apply("Friuts",
                        Create.of(KV.of("australia", "cherry"),
                                  KV.of("brazil", "apple"),
                                  KV.of("canada", "banan"))
);
```

Change `WordsAlphabet` to `ProductWeight`:
```
static class ProductWeight {

        private String country;
        private String fruit;
        private Integer productWeight;

        public WordsAlphabet(String country, String fruit, Integer productWeight) {
            this.country = country;
            this.fruit = fruit;
            this.productWeight = productWeight;
        }
        
        // ToString...
}
```

The union takes place through the keys:
```
static PCollection<String> applyTransform(PCollection<String> fruits, PCollection<String> countries) {

        TupleTag<String> fruitsTag = new TupleTag<>();
        TupleTag<String> productWeightTag = new TupleTag<>();

        MapElements<String, KV<String, String>> mapToAlphabetKv =
                MapElements.into(kvs(strings(), strings()))
                        .via(word -> KV.of(word.substring(0, 1), word));

        PCollection<KV<String, String>> fruitsPColl = fruits.apply("Fruit to KV", mapToAlphabetKv);
        PCollection<KV<String, String>> countriesPColl = countries
                .apply("Country to KV", mapToAlphabetKv);

        return KeyedPCollectionTuple
                .of(fruitsTag, fruitsPCollection)
                .and(productWeightTag, weightPCollection)
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, String>() {

                    @ProcessElement
                    public void processElement(
                            @Element KV<String, CoGbkResult> element, OutputReceiver<String> out) {

                        String alphabet = element.getKey();
                        CoGbkResult coGbkResult = element.getValue();

                        String fruit = coGbkResult.getOnly(fruitsTag);
                        String country = coGbkResult.getOnly(countriesTag);

                        out.output(new WordsAlphabet(alphabet, fruit, country).toString());
                    }

                }));
}
```

