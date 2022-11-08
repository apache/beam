# ParDo one-to-many

It works like ParDo one-to-one, but inside the logic you can do complex operations like dividing the list into separate elements and processing

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

### Description for example

At the input, the elements of the `PCollection` are represented as strings. The `applyTransform()` function returns a list of words that make up a sentence.