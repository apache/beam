You can solve the challenge with this way:
1. Complete the compositions function first of all filters all words that begin with "s" in lowercase `return strings.HasPrefix(strings.ToLower(element),"s")`. Then, using `stats.Count(s, wordsStartWithLetterS)`, you can group the words so that they are unique. After that, we return only the key `beam.ParDo(s, func(word string, count int) string {return word}`.
2. To return two `PCollection` we need to use `ParDo2` with nested logic in this case checking for the case of letters:
```
beam.ParDo2(s, func(element string, wordWithUpperCase, wordWithLowerCase func(string)) {
if strings.HasPrefix(element,"S") {
   wordWithUpperCase(element)
   return
}
wordWithLowerCase(element)
}, input)

```

3. To check whether words with **lowercase** letters are contained in words with **uppercase** letters, you need to create a `view` using `side-input`.
```
option := beam.SideInput{
	Input: wordWithLowerCase,
}
```

4. After that, you need to check for the presence of:
```
var word string
for wordWithLowerCase(&word) {
    if strings.ToLower(wordWithUpperCase) == word {
        emit(word)
	    break
	}
}
```