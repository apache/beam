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

### Common Transforms motivating challenge
You are given a `PCollection` constructed from in-memory array of integer numbers: `[12, -34, -1, 0, 93, -66, 53, 133, -133, 6, 13, 15]`. Your task is to count how many positive even numbers and how many positive odd numbers are there. Although there are many ways how to do this try to use different transformas introduced in this module.

Please note that 0 is an even number.

Considering all of the above, in a given array you have 3 positive even numbers (12, 0 and 6) and 5 positive odd numbers (93, 53, 133, 13, 15).