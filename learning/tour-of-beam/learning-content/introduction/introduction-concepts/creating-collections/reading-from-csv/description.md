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

### Read from csv file

Data processing pipelines often work with tabular data. In many examples and challenges throughout the course, you’ll be working with one of the datasets stored as csv files in either beam-examples, dataflow-samples buckets.

Loading data from csv file requires some processing and consists of two main part:
* Loading text lines using `TextIO.Read` transform
* Parsing lines of text into tabular format

### Playground exercise

Try to experiment with an example in the playground window and modify the code to process other fields from New York taxi rides dataset.

Here is a small list of fields and an example record from this dataset:

| cost | passenger_count | ... |
|------|-----------------|-----|
| 5.8  | 1               | ... |
| 4.6  | 2               | ... |
| 24   | 1               | ... |

Overview [file](https://storage.googleapis.com/apache-beam-samples/nyc_taxi/misc/sample1000.csv)
