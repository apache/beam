---
title:  "So You Want to Write Tests on Your Beam Pipeline?"
date:   2024-07-08 00:00:01 -0800
categories:
  - blog
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
## So You Want to Write Tests on your Beam pipeline?
Testing remains one of the most fundamental components of software engineering. In this blog post, we shed light on some of the constructs that Apache Beam provides to allow for testing. We cover an opinionated set of best practices to write unit tests for your data pipeline in this post. Note that this post does not include integration tests, and those should be authored separately. The examples used in this post are in Python, but the concepts translate broadly across SDKs.

Suppose we write a particular PTransform that reads data from a CSV file, gets passed through a custom function for parsing, and is written back to another Google Cloud Storage bucket (we need to do some custom data formatting to have data prepared for a downstream application).


The pipeline is structured as follows:

    #The following packages are used to run the example pipelines
    import apache_beam as beam
    import apache_beam.io.textio.ReadFromText
    import apache_beam.io.textio.WriteToText

    with beam.Pipeline(argv=self.args) as p:
        result = p | ReadFromText("gs://my-storage-bucket/csv_location.csv")
                   | beam.ParDo(lambda x: custom_function(x))
                   | WriteToText("gs://my-output-bucket-location/")

We then add a custom function to our code:

    def custom_function(x):
         ...
         returned_record = requests.get("http://my-api-call.com")
         ...
         if len(returned_record)!=10:
            raise ValueError("Length of record does not match expected length")

In this scenario, we recommend the following best practices:

1. You don’t need to write any unit tests for the already supported connectors in the Beam Library, such as ReadFromBigQuery and WriteToGCS. These connectors are already tested in Beam’s test suite to ensure correct functionality.
2. You should write unit tests for any custom operations introduced in the pipeline, such as Map, FlatMap, Filter, ParDo, and so on. We recommend adding tests for any lambda functions utilized within these Beam primitives. Additionally, even if you’re using a predefined function, treat the entire transform as a unit, and test it.

### Example Pipeline 1
Let’s use the following pipeline as an example. Because we have a function, we should write a unit test to ensure that our function works as intended.

    def compute_square(element):
        return element**2

    with beam.Pipeline(argv=self.args) as p1:
        result = p1 | ReadFromText("gs://my-storage-bucket/csv_location.csv")
                    | beam.Map(compute_square)
                    | WriteToText("gs://my-output-bucket-location/")

### Example Pipeline 2

Now let’s use the following pipeline as another example. Because we use a predefined function, we don’t need to unit test the function, as `str.strip`, is tested elsewhere. However, we do need to test the output of the `beam.Map` function.

    with beam.Pipeline(argv=self.args) as p2:
        result = p2 | ReadFromText("gs://my-storage-bucket/csv_location.csv")
                    | beam.Map(str.strip)
                    | WriteToText("gs://my-output-bucket-location/")


Here are the corresponding tests for both pipelines:

    # The following packages are imported for unit testing.
    import unittest
    import apache_beam as beam


    @unittest.skipIf(HttpError is None, 'GCP dependencies are not installed')
    class TestBeam(unittest.TestCase):

    # This test corresponds to pipeline p1, and is written to confirm the compute_square function works as intended.
        def test_compute_square(self):
            numbers=[1,2,3]


        with TestPipeline() as p:
            output = p | beam.Create([1,2,3])
                       | beam.Map(compute_square)
        assert_that(output, equal_to([1,4,9]))



    # This test corresponds to pipeline p2, and is written to confirm that our map operation works as intended.
 	def test_strip_map(self):
		strings= [' Strawberry   \n','   Carrot   \n','   Eggplant   \n']
		with TestPipeline() as p:
			output = p | beam.Create(strings)
                               | beam.Map(str.strip)
        assert_that(output,['Strawberry','Carrot','Eggplant'])



The following cover other testing best practices:

1. Test all error messages you raise.
2. Cover any edge cases that might be present in your data.
3. Notice that in pipeline 1, we could have written the `beam.Map` step as  `beam.Map(lambda x: x**2)`, instead of `beam.Map(compute_square)`. The latter (separating the lambda into a helper function) is the recommended approach for more testable code, as changes to the function would be modularized.
4. Use the `assert_that` statement to ensure that PCollection values match up correctly, such as the following example:


      class TestBeam(unittest.TestCase):
          def test_custom_function(self):
              with TestPipeline() as p:
                input = p | beam.ParDo(custom_function(("1","2","3"))
              assert_that(input, equal_to(["1","2","3"]))


5. If needed, use mocking to mock any API calls that might be present in your ParDo function. The purpose of mocking is to test your functionality extensively, even if this testing requires a specific response from an API call.

The following snippet is based off of example pipeline 1, from the top of this blog post.

      # We import the mock package for mocking functionality
      import mock


      @mock.patch.object(CustomFunction, 'get_record')
      def test_error_message_wrong_length(self, get_record):
        record = ["field1","field2",...]
        get_record.return_value = record
        with self.assertRaisesRegex(ValueError,
                                    "Length of record does not match expected length'"):
            p = apache_beam.Pipeline()
            result = p | beam.ParDo(custom_function(x))
        result






For more pointed guidance on testing on Beam/Dataflow, see the [Google Cloud documentation](https://cloud.google.com/dataflow/docs/guides/develop-and-test-pipelines).

Special thanks to Danny McCormick, XQ Hu, Surjit Singh, and Rebecca Spzer, who helped refine the ideas in this post.

