spark-dataflow
==============
Spark-dataflow allows users to execute dataflow pipelines with Spark. Executing a pipeline on a spark cluster is easy: Depend on spark-dataflow in your project
and execute your pipeline in a program by calling `SparkPipelineRunner.run`.

The Maven coordinates of the current version of this project are:

    <groupId>com.cloudera.dataflow.spark</groupId>
    <artifactId>dataflow-spark</artifactId>
    <version>0.0.1</version>

An example of running a pipeline against a spark cluster in local mode with 2
threads.

    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());
    /** logic for building your pipeline */
    EvaluationResult result = new SparkPipelineRunner("local[2]").run()


