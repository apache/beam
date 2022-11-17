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