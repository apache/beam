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

# CoGroup

A transform that performs equijoins across multiple schema PCollections.

This transform has similarities to `CoGroupByKey`, however works on PCollections that have schemas. This allows users of the transform to simply specify schema fields to join on. The output type of the transform is Row that contains one row field for the key and an ITERABLE field for each input containing the rows that joined on that key; by default the cross product is not expanded, but the cross product can be optionally expanded. By default the key field is named "key" (the name can be overridden using `withKeyField`) and has index 0. The tags in the `PCollectionTuple` control the names of the value fields in the `Row`.