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
### Final challenge 2

You are given a file analyzed.csv which maps words to sentiments. Using this, analyze kinglear.txt. Output PCollections counting the number of **negative words** and **positive words**  as well as PCollections counting the number of **positive words with strong modal** and **positive words with weak modal**.

Example rows from input file:

| Word         | Negative | Positive | Uncertainty | Litigious | Strong_Modal | Weak_Modal | Constraining |
|--------------|----------|----------|-------------|-----------|--------------|------------|--------------|
| NONSEVERABLE | 0        | 0        | 0           | 2011      | 0            | 0          | 0            |
| DISFAVOR     | 2009     | 0        | 0           | 0         | 0            | 0          | 0            |
| COMPLIMENT   | 0        | 2009     | 0           | 0         | 0            | 0          | 0            |