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