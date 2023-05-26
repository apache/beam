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

You have a transaction file with data. You need to parse the csv file. And write a **Pojo class** and `Coder` for it so that Pipeline understands which object it is working with. Make a report that is sent every 30 seconds. Filter so that the **quantity** of the product is more than 20. And divide into 2 parts. In the first part, the transaction **price** should be more than 10, the second less. Combine so that for each transaction id there is a summed price. And write in two files for **"price more than 10"** and **"price less than 10"**.

| TransactionNo | Date      | ProductNo | ProductName                         | Price | Quantity | CustomerNo | Country        |
|---------------|-----------|-----------|-------------------------------------|-------|----------|------------|----------------|
| 581482        | 12/9/2019 | 22485     | Set Of 2 Wooden Market Crates       | 21    | 47       | 17490      | United Kingdom |
| 581475        | 12/9/2019 | 22596     | Christmas Star Wish List Chalkboard | 10.65 | 36       | 13069      | United Kingdom |
| 581475        | 12/9/2019 | 23235     | Storage Tin Vintage Leaf            | 11.53 | 12       | 13069      | United Kingdom |      
