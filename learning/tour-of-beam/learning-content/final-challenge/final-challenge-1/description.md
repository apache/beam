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
### Final challenge 1

You’re given a csv file with purchase transactions. Write a Beam pipeline to prepare a report every 30 seconds. The report needs to be created only for transactions where quantity is more than 20.

Report should consist of two files named "**price_more_than_10.txt**" and "**price_less_than_10.txt**":

* Total transactions amount grouped by **ProductNo** for products with **price** greater than 10
* Total transactions amount grouped by **ProductNo** for products with **price** less than 10

Example rows from input file:

| TransactionNo | Date      | ProductNo | ProductName                         | Price | Quantity | CustomerNo | Country        |
|---------------|-----------|-----------|-------------------------------------|-------|----------|------------|----------------|
| 581482        | 12/9/2019 | 22485     | Set Of 2 Wooden Market Crates       | 21    | 47       | 17490      | United Kingdom |
| 581475        | 12/9/2019 | 22596     | Christmas Star Wish List Chalkboard | 10.65 | 36       | 13069      | United Kingdom |
| 581475        | 12/9/2019 | 23235     | Storage Tin Vintage Leaf            | 11.53 | 12       | 13069      | United Kingdom |