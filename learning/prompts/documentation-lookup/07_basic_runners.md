Prompt:
What is a Runner in Apache Beam?

Response:
Apache Beam runners are the execution engines that run the pipelines. They translate or adapt the pipeline into a form that can be executed on a massively parallel big data processing system, such as Apache Flink, Apache Spark, Google Cloud Dataflow, and more.

Choosing a runner is an important step in the pipeline development process. The runner you choose determines where and how your pipeline runs. For more information about available runners and their capabilities, see the [Beam Capability Matrix](https://beam.apache.org/documentation/runners/capability-matrix/).

To specify a runner, when you execute the pipeline, use the `--runner` flag. For example, to run the WordCount pipeline on Google Cloud Dataflow, you would run the following command:

```python
python -m apache_beam.examples.wordcount \
  --region DATAFLOW_REGION \
  --input gs://dataflow-samples/shakespeare/kinglear.txt \
  --output gs://STORAGE_BUCKET/results/outputs \
  --runner DataflowRunner \
  --project PROJECT_ID \
  --temp_location gs://STORAGE_BUCKET/tmp/
  ```

For more details about the WordCount example, see the [WordCount source code in GitHub](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/wordcount.py).

The [Direct Runner](https://beam.apache.org/documentation/runners/direct/) executes the pipeline on your local machine. It is helpful for testing and debugging purposes.

For instructions about how to configure your development environment and runner, see the quickstarts for [Java](https://beam.apache.org/get-started/quickstart-java), [Python](https://beam.apache.org/get-started/quickstart-py), and [Go](https://beam.apache.org/get-started/quickstart-go).
