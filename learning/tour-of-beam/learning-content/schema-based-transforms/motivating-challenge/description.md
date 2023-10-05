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

### Schema Based Transforms motivating challenge

You are provided with a `PCollection` of the array of game statistics of users in a csv file. And a `PCollection` of user data. You need to group them by **summing up all** the points. And output users who have more than **11** points.Don't forget to write a `coder`.


`Game PCollection`:
| userId                   | score | gameId        | date                |
|--------------------------|-------|---------------|---------------------|
| user16_AmaranthKoala     | 18    | 1447719060000 | 2015-11-16 16:11:04 |
| user10_AndroidGreenKoala | 2     | 1447719060000 | 2015-11-16 16:11:04 |
| user9_AuburnCockatoo     | 5     | 1447719060000 | 2015-11-16 16:11:04 |
| ...                      | ...   | ...           | ...                 |



`User PCollection`:
| userId                   | userName          |
|--------------------------|-------------------|
| user16_AmaranthKoala     | AmaranthKoala     |
| user10_AndroidGreenKoala | AndroidGreenKoala |
| user9_AuburnCockatoo     | AuburnCockatoo    |
| ...                      | ...               |


Overview [file](https://storage.googleapis.com/apache-beam-samples/game/small/gaming_data.csv)