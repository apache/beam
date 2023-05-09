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
### Core Transforms motivating challenge-3

You are given the work of Shakespeare "Kinglear", it will be divided into words and filtered.  Leave the words beginning with "**i**", the case does not matter. After using the `additional output`, return the two `PCollection` separation conditions will be **uppercase** and **lowercase**. Create a view from the PCollection with **lowercase** words and passing it to the `side-input` check if there are matching words in both registers.