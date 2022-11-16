# Additional outputs

While `beam.ParDo` always produces an output `PCollection`, your `DoFn` can produce any number of additional output `PCollection`s, or even none at all. If you choose to have multiple outputs, your `DoFn` needs to be called with the `ParDo` function that matches the number of outputs. `beam.ParDo2` for two output `PCollection`s, `beam.ParDo3` for three and so on until `beam.ParDo7`. If you need more, you can use `beam.ParDoN` which will return a `[]beam.PCollection`.

### Tags for multiple outputs

The Go SDK doesn’t use output tags, and instead uses positional ordering for multiple output `PCollection`s.

```
// beam.ParDo3 returns PCollections in the same order as
// the emit function parameters in processWords.
below, above, marked := beam.ParDo3(s, processWords, words)

// processWordsMixed uses both a standard return and an emitter function.
// The standard return produces the first PCollection from beam.ParDo2,
// and the emitter produces the second PCollection.
length, mixedMarked := beam.ParDo2(s, processWordsMixed, words)
```

### Emitting to multiple outputs in your DoFn

Call emitter functions as needed to produce 0 or more elements for its matching `PCollection`. The same value can be emitted with multiple emitters. As normal, do not mutate values after emitting them from any emitter.

All emitters should be registered using a generic `register.EmitterX[...]` function. This optimizes runtime execution of the emitter.

`DoFn`s can also return a single element via the standard return. The standard return is always the first PCollection returned from `beam.ParDo`. Other emitters output to their own `PCollection`s in their defined parameter order.

```
// processWords is a DoFn that has 3 output PCollections. The emitter functions
// are matched in positional order to the PCollections returned by beam.ParDo3.
func processWords(word string, emitBelowCutoff, emitAboveCutoff, emitMarked func(string)) {
	const cutOff = 5
	if len(word) < cutOff {
		emitBelowCutoff(word)
	} else {
		emitAboveCutoff(word)
	}
	if isMarkedWord(word) {
		emitMarked(word)
	}
}

// processWordsMixed demonstrates mixing an emitter, with a standard return.
// If a standard return is used, it will always be the first returned PCollection,
// followed in positional order by the emitter functions.
func processWordsMixed(word string, emitMarked func(string)) int {
	if isMarkedWord(word) {
		emitMarked(word)
	}
	return len(word)
}

func init() {
	register.Function4x0(processWords)
	register.Function2x1(processWordsMixed)
	// 1 input of type string => Emitter1[string]
	register.Emitter1[string]()
}
```

### Accessing additional parameters in your DoFn

In addition to the element, Beam will populate other parameters to your DoFn’s `ProcessElement` method. Any combination of these parameters can be added to your process method in a standard order.

**context.Context**: To support consolidated logging and user defined metrics, a `context.Context` parameter can be requested. Per Go conventions, if present it’s required to be the first parameter of the `DoFn` method.

```
func MyDoFn(ctx context.Context, word string) string { ... }
```

**Timestamp**: To access the timestamp of an input element, add a `beam.EventTime` parameter before the element. For example:

```
func MyDoFn(ts beam.EventTime, word string) string { ... }
```

**Window**: To access the window an input element falls into, add a `beam.Window` parameter before the element. If an element falls in multiple windows (for example, this will happen when using `SlidingWindows`), then the `ProcessElement` method will be invoked multiple time for the element, once for each window. Since `beam.Window` is an interface it’s possible to type assert to the concrete implementation of the window. For example, when fixed windows are being used, the window is of type `window.IntervalWindow`.

```
func MyDoFn(w beam.Window, word string) string {
  iw := w.(window.IntervalWindow)
  ...
}
```

**PaneInfo**: When triggers are used, Beam provides `beam.PaneInfo` object that contains information about the current firing. Using `beam.PaneInfo` you can determine whether this is an early or a late firing, and how many times this window has already fired for this key.

```
func extractWordsFn(pn beam.PaneInfo, line string, emitWords func(string)) {
	if pn.Timing == typex.PaneEarly || pn.Timing == typex.PaneOnTime {
		// ... perform operation ...
	}
	if pn.Timing == typex.PaneLate {
		// ... perform operation ...
	}
	if pn.IsFirst {
		// ... perform operation ...
	}
	if pn.IsLast {
		// ... perform operation ...
	}

	words := strings.Split(line, " ")
	for _, w := range words {
		emitWords(w)
	}
}
```

### Description for example 

At the input is integers. The `applyTransform()` accepts a list of integers at the output two `PCollection` one `PCollection` above 100 and second below 100.
