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
1. Filter whether the number is equal to zero or greater than it using `Filter`
2. Set the "odd" key for odd numbers and "even" for even numbers using `WithKeys`
3. Count the number of even and odd numbers using the `Count.PerKey()`