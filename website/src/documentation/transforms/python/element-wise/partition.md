---
layout: section
title: "Partition"
permalink: /documentation/transforms/python/elementwise/partition/
section_menu: section-menu/documentation.html
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

<script type="text/javascript">
localStorage.setItem('language', 'language-py')
</script>

<table>
  <td>
    <a class="button" target="_blank"
        href="https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html#apache_beam.transforms.core.Partition">
      <img src="https://beam.apache.org/images/logos/sdks/python.png"
          width="20px" height="20px" alt="Pydoc" />
      Pydoc
    </a>
  </td>
</table>
<br>
Separates elements in a collection into multiple output
collections. The partitioning function contains the logic that determines how
to separate the elements of the input collection into each resulting
partition output collection.

The number of partitions must be determined at graph construction time.
You cannot determine the number of partitions in mid-pipeline

See more information in the [Beam Programming Guide]({{ site.baseurl }}/documentation/programming-guide/#partition).

## Examples

In the following examples, we create a pipeline with a `PCollection` of produce with their icon, name, and duration.
Then, we apply `Partition` in multiple ways to split the `PCollection` into multiple `PCollections`.

`Partition` accepts a function that receives the number of partitions,
and returns the index of the desired partition for the element.
The number of partitions passed must be a positive integer,
and it must return an integer in the range `0` to `num_partitions-1`.

### Example 1: Partition with a function

In the following example, we have a known list of durations.
We partition the `PCollection` into one `PCollection` for every duration type.

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/partition.py tag:partition_function %}```

Output `PCollection`s:

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/partition_test.py tag:partitions %}```

<table>
  <td>
    <a class="button" target="_blank"
        href="https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/partition.py">
      <img src="https://www.tensorflow.org/images/GitHub-Mark-32px.png"
        width="20px" height="20px" alt="View on GitHub" />
      View on GitHub
    </a>
  </td>
</table>
<br>

### Example 2: Partition with a lambda function

We can also use lambda functions to simplify **Example 1**.

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/partition.py tag:partition_lambda %}```

Output `PCollection`s:

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/partition_test.py tag:partitions %}```

<table>
  <td>
    <a class="button" target="_blank"
        href="https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/partition.py">
      <img src="https://www.tensorflow.org/images/GitHub-Mark-32px.png"
        width="20px" height="20px" alt="View on GitHub" />
      View on GitHub
    </a>
  </td>
</table>
<br>

### Example 3: Partition with multiple arguments

You can pass functions with multiple arguments to `Partition`.
They are passed as additional positional arguments or keyword arguments to the function.

In machine learning, it is a common task to split data into
[training and a testing datasets](https://en.wikipedia.org/wiki/Training,_validation,_and_test_sets).
Typically, 80% of the data is used for training a model and 20% is used for testing.

In this example, we split a `PCollection` dataset into training and testing datasets.
We define `split_dataset`, which takes the `plant` element, `num_partitions`,
and an additional argument `ratio`.
The `ratio` is a list of numbers which represents the ratio of how many items will go into each partition.
`num_partitions` is used by `Partitions` as a positional argument,
while `plant` and `ratio` are passed to `split_dataset`.

If we want an 80%/20% split, we can specify a ratio of `[8, 2]`, which means that for every 10 elements,
8 go into the first partition and 2 go into the second.
In order to determine which partition to send each element, we have different buckets.
For our case `[8, 2]` has **10** buckets,
where the first 8 buckets represent the first partition and the last 2 buckets represent the second partition.

First, we check that the ratio list's length corresponds to the `num_partitions` we pass.
We then get a bucket index for each element, in the range from 0 to 9 (`num_buckets-1`).
We could do `hash(element) % len(ratio)`, but instead we sum all the ASCII characters of the
JSON representation to make it deterministic.
Finally, we loop through all the elements in the ratio and have a running total to
identify the partition index to which that bucket corresponds.

This `split_dataset` function is generic enough to support any number of partitions by any ratio.
You might want to adapt the bucket assignment to use a more appropriate or randomized hash for your dataset.

```py
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/partition.py tag:partition_multiple_arguments %}```

Output `PCollection`s:

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/partition_test.py tag:train_test %}```

<table>
  <td>
    <a class="button" target="_blank"
        href="https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/transforms/element_wise/partition.py">
      <img src="https://www.tensorflow.org/images/GitHub-Mark-32px.png"
        width="20px" height="20px" alt="View on GitHub" />
      View on GitHub
    </a>
  </td>
</table>
<br>

## Related transforms

* [Filter]({{ site.baseurl }}/documentation/transforms/python/elementwise/filter) is useful if the function is just
  deciding whether to output an element or not.
* [ParDo]({{ site.baseurl }}/documentation/transforms/python/elementwise/pardo) is the most general element-wise mapping
  operation, and includes other abilities such as multiple output collections and side-inputs.
* [CoGroupByKey]({{ site.baseurl }}/documentation/transforms/python/aggregation/cogroupbykey)
performs a per-key equijoin.

<table>
  <td>
    <a class="button" target="_blank"
        href="https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html#apache_beam.transforms.core.Partition">
      <img src="https://beam.apache.org/images/logos/sdks/python.png"
          width="20px" height="20px" alt="Pydoc" />
      Pydoc
    </a>
  </td>
</table>
<br>
