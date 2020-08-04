---
title: "Partition"
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
# Partition
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/transforms/Partition.html">
      <img src="https://beam.apache.org/images/logos/sdks/java.png" width="20px" height="20px"
           alt="Javadoc" />
     Javadoc
    </a>
</table>
<br><br>

Separates elements in a collection into multiple output collections. The partitioning function contains the logic that determines how to separate the elements of the input collection into each resulting partition output collection.

The number of partitions must be determined at graph construction time. You cannot determine the number of partitions in mid-pipeline.

See more information in the [Beam Programming Guide](/documentation/programming-guide/#partition).

## Examples
**Example**: dividing a `PCollection` into percentile groups

{{< highlight java >}}
// Provide an int value with the desired number of result partitions, and a PartitionFn that represents the
// partitioning function. In this example, we define the PartitionFn in-line. Returns a PCollectionList
// containing each of the resulting partitions as individual PCollection objects.
PCollection<Student> students = ...;
// Split students up into 10 partitions, by percentile:
PCollectionList<Student> studentsByPercentile =
    students.apply(Partition.of(10, new PartitionFn<Student>() {
        public int partitionFor(Student student, int numPartitions) {
            return student.getPercentile()  // 0..99
                 * numPartitions / 100;
        }}));

// You can extract each partition from the PCollectionList using the get method, as follows:
PCollection<Student> fortiethPercentile = studentsByPercentile.get(4);
{{< /highlight >}}

## Related transforms 
* [Filter](/documentation/transforms/java/elementwise/filter) is useful if the function is just 
  deciding whether to output an element or not.
* [ParDo](/documentation/transforms/java/elementwise/pardo) is the most general element-wise mapping
  operation, and includes other abilities such as multiple output collections and side-inputs. 
* [CoGroupByKey](/documentation/transforms/java/aggregation/cogroupbykey)
  performs a per-key equijoin. 