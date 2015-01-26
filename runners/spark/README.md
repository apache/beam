spark-dataflow
==============

Spark-dataflow is an early prototype. If this project interests you, you should know that we
encourage outside contributions. So, hack away! To get an idea of what we have already identified as
areas that need improvement, checkout the issues listed in the github repo.

Spark-dataflow allows users to execute dataflow pipelines with Spark. Executing a pipeline on a
spark cluster is easy: Depend on spark-dataflow in your project and execute your pipeline in a
program by calling `SparkPipelineRunner.run`.

The Maven coordinates of the current version of this project are:

    <groupId>com.cloudera.dataflow.spark</groupId>
    <artifactId>spark-dataflow</artifactId>
    <version>0.0.1</version>
    
and are hosted in Cloudera's repository at:

<repository>
  <id>cloudera.repo</id>
  <url>https://repository.cloudera.com/artifactory/cloudera-repos</url>
</repository>

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
