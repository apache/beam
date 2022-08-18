### Lightweight DoFns and other abstractions

If your function is relatively straightforward, you can simplify your use of ```ParDo``` by providing a lightweight ```DoFn``` in-line, as an anonymous function .

Here’s the previous example, ```ParDo``` with ComputeLengthWordsFn, with the ```DoFn``` specified as an anonymous function :

```
// words is the input PCollection of strings
var words beam.PCollection = ...

lengths := beam.ParDo(s, func (word string, emit func(int)) {
      emit(len(word))
}, words)
```

If your ```ParDo``` performs a one-to-one mapping of input elements to output elements–that is, for each input element, it applies a function that produces exactly one output element, you can return that element directly.
Here’s the previous example using a direct return:

```
// words is the input PCollection of strings
var words beam.PCollection = ...


// Apply an anonymous function as a DoFn PCollection words.
// Save the result as the PCollection wordLengths.
wordLengths := beam.ParDo(s, func(word string) int {
	return len(word)
}, words)
```