---
title: "CoGroupByKey"
---
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
# CoGroupByKey
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/transforms/join/CoGroupByKey.html">
      <img src="https://beam.apache.org/images/logos/sdks/java.png" width="20px" height="20px"
           alt="Javadoc" />
     Javadoc
    </a>
</table>
<br><br>

Aggregates all input elements by their key and allows downstream processing
to consume all values associated with the key. While `GroupByKey` performs
this operation over a single input collection and thus a single type of
input values, `CoGroupByKey` operates over multiple input collections. As
a result, the result for each key is a tuple of the values associated with
that key in each input collection.

See more information in the [Beam Programming Guide](/documentation/programming-guide/#cogroupbykey).

## Examples
**Example**: Say you have two different files with user data; one file has
names and email addresses and the other file has names and phone numbers.

You can join those two data sets, using the username as a common key and the
other data as the associated values. After the join, you have one data set
that contains all of the information (email addresses and phone numbers)
associated with each name.

{{< highlight java >}}
PCollection<KV<UID, Integer>> pt1 = /* ... */;
PCollection<KV<UID, String>> pt2 = /* ... */;

final TupleTag<Integer> t1 = new TupleTag<>();
final TupleTag<String> t2 = new TupleTag<>();
PCollection<KV<UID, CoGBKResult>> result =
  KeyedPCollectionTuple.of(t1, pt1).and(t2, pt2)
    .apply(CoGroupByKey.create());
result.apply(ParDo.of(new DoFn<KV<K, CoGbkResult>, /* some result */>() {
  @ProcessElement
  public void processElement(ProcessContext c) {
    KV<K, CoGbkResult> e = c.element();
    CoGbkResult result = e.getValue();
    // Retrieve all integers associated with this key from pt1
    Iterable<Integer> allIntegers = result.getAll(t1);
    // Retrieve the string associated with this key from pt2.
    // Note: This will fail if multiple values had the same key in pt2.
    String string = e.getOnly(t2);
    ...
}));
{{< /highlight >}}

## Related transforms
* [GroupByKey](/documentation/transforms/java/aggregation/groupbykey)
  takes one input collection.
