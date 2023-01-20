**Hint**

You can use the following code snippet to create an input PCollection:
{{if (eq .Sdk "go")}}
Don't forget to add import:

```
import (
    "strings"
    ...
)
```

Create data for PCollection:

```
str:= "To be, or not to be: that is the question: Whether 'tis nobler in the mind to suffer The slings and arrows of outrageous fortune, Or to take arms against a sea of troubles, And by opposing end them. To die: to sleep"

input := beam.CreateList(s,strings.Split(str, " "))
```

And filtering:

```
func applyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
    return filter.Include(s, input, func(word string) bool {
		return strings.HasPrefix(strings.ToUpper(word), "A")
    })
}
```
{{end}}
{{if (eq .Sdk "java")}}
Don't forget to add import:

```
import java.util.Arrays;
```

Create data for PCollection:

```
String str = "To be, or not to be: that is the question:Whether 'tis nobler in the mind to suffer The slings and arrows of outrageous fortune,Or to take arms against a sea of troubles,And by opposing end them. To die: to sleep";

PCollection<String> input = pipeline.apply(Create.of(Arrays.asList(str.split(" "))));
```

And filtering:

```
static PCollection<String> applyTransform(PCollection<String> input) {
        return input.apply(Filter.by(word -> word.toUpperCase().startsWith("A")));
}
```
{{end}}
{{if (eq .Sdk "python")}}
```
(p | beam.Create(["To be, or not to be: that is the question:Whether 'tis nobler in the mind to suffer The slings and arrows of outrageous fortune,Or to take arms against a sea of troubles,And by opposing end them. To die: to sleep"])
  | beam.ParDo(SplitWords())
  | beam.Filter(lambda word: word.upper().startswith("A"))
  | Output(prefix='PCollection filtered value: '))
```

For split word you can use:

```
class SplitWords(beam.DoFn):
  def __init__(self, delimiter=' '):
    self.delimiter = delimiter

  def process(self, text):
    for word in text.split(self.delimiter):
      yield word
```
{{end}}
