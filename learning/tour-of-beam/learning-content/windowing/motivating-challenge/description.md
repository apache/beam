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

### Windowing Transforms motivating exercise

For this exercise, you can use NYC taxi trips csv containing a list of trips and their prices. Following is a small list of fields and an example record from this dataset:

| cost | passenger_count | ... |
|------|-----------------|-----|
| 5.8  | 1               | ... |
| 4.6  | 2               | ... |
| 24   | 1               | ... |

Your task is to write a pipeline that returns the maximum price of taxi trips for the past 10 min. Calculations need to be updated every minute.

Overview [file](https://storage.googleapis.com/apache-beam-samples/nyc_taxi/misc/sample1000.csv)