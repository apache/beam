---
title:  "Unit Testing in Beam: An opinionated guide"
date:   2024-09-09 00:00:01 -0800
categories:
  - blog
aliases:
  - /blog/2024/09/09/unit-testing-in-beam.html
authors:
  - svetakvsundhar
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

Testing remains one of the most fundamental components of software engineering. In this blog post, we shed light on some of the constructs that Apache Beam provides to allow for testing.
We cover an opinionated set of best practices to write unit tests for your data pipeline in this post. Note that this post does not include integration tests, and those should be authored separately.
All snippets in this post are included in [this notebook](https://github.com/apache/beam/blob/master/examples/notebooks/blog/unittests_in_beam.ipynb). Additionally, please take a look at the [Beam starter projects](https://beam.apache.org/blog/beam-starter-projects/), as these contain tests that exhibit best practices.



####When testing Beam pipelines, we recommend the following best practices:

1) You don’t need to write any unit tests for the already supported connectors in the Beam Library, such as `ReadFromBigQuery` and `WriteToText`. These connectors are already tested in Beam’s test suite to ensure correct functionality and add unnecessary cost and dependencies to a unit test.

2) You should ensure your function is well tested when using it with `Map`, `FlatMap`, or `Filter`. You can assume your function will work as intended when using `Map(your_function)`.
3) For more complex transforms such as `ParDo`’s, side inputs, timestamp inspection, etc., treat the entire transform as a unit, and test it.
4) If needed, use mocking to mock any API calls that might be present in your DoFn. The purpose of mocking is to test your functionality extensively, even if this testing requires a specific response from an API call.

   1) Be sure to modularize your API calls in separate functions, rather than making the API call directly in the `DoFn`. This will allow for a cleaner experience when mocking the external API calls.


###Example 1

Let’s use the following pipeline as an example. We do not have to write a separate unit test to test this function in the context of this pipeline, assuming the function  `median_house_value_per_bedroom` is unit tested elsewhere in the code. We can trust that the Map primitive will work as expected (this illustrates point #2 from above).

```python
# The following code computes the median house value per bedroom

with beam.Pipeline() as p1:
   result = (
       p1
       | ReadFromText("/content/sample_data/california_housing_test.csv",skip_header_lines=1)
       | beam.Map(median_house_value_per_bedroom)
       | WriteToText("/content/example2")
   )
```

###Example 2

Now let’s use the following function as our example. The functions `median_house_value_per_bedroom`, and `multiply_by_factor` are tested elsewhere, but the pipeline as a whole (which consists of composite transforms) is not.

```python
with beam.Pipeline() as p2:
    result = (
        p2
        | ReadFromText("/content/sample_data/california_housing_test.csv",skip_header_lines=1)
        | beam.Map(median_house_value_per_bedroom)
        | beam.Map(multiply_by_factor)
        | beam.CombinePerKey(sum)
        | WriteToText("/content/example3")
    )
```

The best practice for the above is to create a transform with all functions between the  `ReadFromText` and `WriteToText`.This will separate the transformation logic from the IOs, allowing us to unit-test the transformation logic. The following is a refactoring of the code above:

```python
def transform_data_set(pcoll):
  return (pcoll
          | beam.Map(median_house_value_per_bedroom)
          | beam.Map(multiply_by_factor)
          | beam.CombinePerKey(sum))

# Define a new class that inherits from beam.PTransform
class MapAndCombineTransform(beam.PTransform):
  def expand(self, pcoll):
    return transform_data_set(pcoll)

with beam.Pipeline() as p2:
   result = (
       p2
       | ReadFromText("/content/sample_data/california_housing_test.csv",skip_header_lines=1)
       | MapAndCombineTransform()
       | WriteToText("/content/example3")
   )
```

Here is the corresponding unit test for the above example:

```python
import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to


class TestBeam(unittest.TestCase):

# This test corresponds to example 3, and is written to confirm the pipeline works as intended.
  def test_transform_data_set(self):
    expected=[(1, 10570.185786231425), (2, 13.375337533753376), (3, 13.315649867374006)]
    input_elements = [
      '-122.050000,37.370000,27.000000,3885.000000,661.000000,1537.000000,606.000000,6.608500,344700.000000',
      '121.05,99.99,23.30,39.5,55.55,41.01,10,34,74.30,91.91',
      '122.05,100.99,24.30,40.5,56.55,42.01,11,35,75.30,92.91',
      '-120.05,39.37,29.00,4085.00,681.00,1557.00,626.00,6.8085,364700.00'
    ]
    with beam.Pipeline() as p2:
      result = (
                p2
                | beam.Create(input_elements)
                | beam.Map(MapAndCombineTransform())
        )
      assert_that(result,equal_to(expected))
```

###Example 3

Suppose we write a pipeline that reads data from a JSON file, gets passed through a custom function that makes external API calls for parsing, and is then written to a custom destination (for example, we need to do some custom data formatting to have data prepared for a downstream application).


The pipeline is structured as follows:

```python
#The following packages are used to run the example pipelines

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

class MyDoFn(beam.DoFn):
  def process(self,element):
          returned_record = MyApiCall.get_data("http://my-api-call.com")
          if len(returned_record)!=10:
            raise ValueError("Length of record does not match expected length")
          yield returned_record

with beam.Pipeline() as p3:
  result = (
          p3
          | ReadFromText("/content/sample_data/anscombe.json")
          | beam.ParDo(MyDoFn())
          | WriteToText("/content/example1")
  )
```

This test checks that if the API response is a record of the wrong length, we throw the expected error.

```python
!pip install mock  # Install the 'mock' module
```
```python
# We import the mock package for mocking functionality.
from unittest.mock import Mock,patch
# from MyApiCall import get_data
import mock


# MyApiCall is a function that calls get_data to fetch some data via an API call.
@patch('MyApiCall.get_data')
def test_error_message_wrong_length(self, mock_get_data):
 response = ['field1','field2']
 mock_get_data.return_value = Mock()
 mock_get_data.return_value.json.return_value=response

 input_elements = ['-122.050000,37.370000,27.000000,3885.000000,661.000000,1537.000000,606.000000,6.608500,344700.000000'] #input length 9
 with self.assertRaisesRegex(ValueError,
                             "Length of record does not match expected length'"):
     p3 = beam.Pipeline()
     result = p3 | beam.create(input_elements) | beam.ParDo(MyDoFn())
     result
```

###The following cover other testing best practices:

1) Test all error messages you raise.
2) Cover any edge cases that might be present in your data.
3) Notice that in example 1, we could have written the `beam.Map` step with lambda functions:

```
beam.Map(lambda x: x.strip().split(',')) | beam.Map(lambda x: float(x[8])/float(x[4])
```

, instead of `beam.Map(median_house_value_per_bedroom)`.  The latter (separating lambdas into a helper function) is the recommended approach for more testable code, as changes to the function would be modularized.
5) Use the `assert_that` statement to ensure that PCollection values match up correctly, such as done above

For more pointed guidance on testing on Beam/Dataflow, see the [Google Cloud documentation](https://cloud.google.com/dataflow/docs/guides/develop-and-test-pipelines). Additionally, see some more examples of unit testing in Beam [here](https://github.com/apache/beam/blob/736cf50430b375d32093e793e1556567557614e9/sdks/python/apache_beam/ml/inference/base_test.py#L262).

Special thanks to Robert Bradshaw, Danny McCormick, XQ Hu, Surjit Singh, and Rebecca Spzer, who helped refine the ideas in this post.

