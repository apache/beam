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

To solve this challenge, you may build a pipeline that consists of the following steps:
1. You need to find the average value using `Mean`.
2. Filter so that the numbers are below average and above. Use `Filter`.
3. Sum up prices that are below average also for prices that are higher. Use `Sum`.
4. Make a map(key-value) using `WithKeys`. The key that will be the number of orders is the value of their sum.
