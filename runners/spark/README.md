spark-dataflow
==============
Spark-dataflow allows users to execute dataflow pipelines with Spark. Executing a pipeline on a
spark cluster is easy: Depend on spark-dataflow in your project and execute your pipeline in a
program by calling `SparkPipelineRunner.run`.

The Maven coordinates of the current version of this project are:

    <groupId>com.cloudera.dataflow.spark</groupId>
    <artifactId>dataflow-spark</artifactId>
    <version>0.0.1</version>

If we wanted to run a dataflow pipeline with the default options of a single threaded spark
instance in local mode, we would do the following:

    Pipeline p = <logic for pipeline creation >
    EvaluationResult result = SparkPipelineRunner.create().run(p);

To create a pipeline runner to run against a different spark cluster, with a custom master url we
would do the following:

    Pipeline p = <logic for pipeline creation >
    SparkPipelineOptions options = SparkPipelineOptionsFactory.create();
    options.setSparkMaster("spark://host:port");
    EvaluationResult result = SparkPipelineRunner.create(options).run(p);
