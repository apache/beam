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