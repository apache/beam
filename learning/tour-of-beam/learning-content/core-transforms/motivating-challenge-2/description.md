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
### Core Transforms motivating challenge-2

You are given a csv file with the players' records, which you need to share with regex.It is necessary to "Sum up" by username using `Combine`, each player's point must be combined(+)

| id                       | username           | score | ... |
|--------------------------|--------------------|-------|-----|
| user16_AmaranthKoala     | AmaranthKoala      | 18    | ... |
| user10_AndroidGreenKoala | AndroidGreenKoala  | 2     | ... |
| user9_AuburnCockatoo     | AuburnCockatoo     | 5     | ... |

Overview [file](https://storage.googleapis.com/apache-beam-samples/game/small/gaming_data.csv)
