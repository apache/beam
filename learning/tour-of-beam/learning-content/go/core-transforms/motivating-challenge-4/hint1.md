You can solve the challenge with this way:
1. Each input is a separate word. We need to make map a key that will be the first letter and the meaning of the word itself. So we have to save to a slice.
```
firsLetterAndWord[string(input[0])] = input

accum.Current = append(accum.Current, firsLetterAndWord)
```

2. In `MergeAccumulators`, check whether there is a result with a letter if there is complement values, if not equate:
```
for _, element := range accum.Current {
    for k, v := range element {
        value, ok := result[k]
        if ok {
            value=append(value,v)
        }
        result[k]=value
    }
}
```