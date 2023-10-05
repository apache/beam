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
1. First you need to convert a collection of users to an `Object`: `.apply(MapElements.into(TypeDescriptor.of(Object.class)).via(it -> it))`.
2. You have to pass the `userSchema` to `setSchema(...)` and write a `TypeDescriptor`. Also pass transformations of `Object to Row` and `Row to Object`.
3. Make a `Join` with `gameInfo` using `userId` field.
4. Make conversions to `Object` using `MapElements` which will return `allFieldSchema`.
5. Write your own coder that accepts `Object`.
6. Make a `setRowSchema()` that accepts `allFieldSchema`.
7. `Group` by `userId` field and **sum up all** `points`.
8. Make conversions to `Object` using `MapElements` which will return `totalSchema`.
9. Make `setCoder()` with your coder.
10. Make a `setRowSchema()` that accepts `totalSchema`.
11. `Filter` by the `total` field where it is **greater than 11**.
