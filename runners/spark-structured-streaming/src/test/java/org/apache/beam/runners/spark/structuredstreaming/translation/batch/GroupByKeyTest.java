package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.spark.structuredstreaming.SparkPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test class for beam to spark {@link ParDo} translation.
 */
@RunWith(JUnit4.class)
public class GroupByKeyTest implements Serializable {
  private static Pipeline pipeline;

  @BeforeClass
  public static void beforeClass(){
    PipelineOptions options = PipelineOptionsFactory.create().as(SparkPipelineOptions.class);
    options.setRunner(SparkRunner.class);
    pipeline = Pipeline.create(options);
  }

  @Ignore("fails with Unable to create serializer "
      + "\"com.esotericsoftware.kryo.serializers.FieldSerializer\" for class: "
      + "worker.org.gradle.internal.UncheckedException in last map step")
  @Test
  public void testGroupByKey(){
    Map<Integer, Integer> elems = new HashMap<>();
    elems.put(1, 1);
    elems.put(1, 3);
    elems.put(1, 5);
    elems.put(2, 2);
    elems.put(2, 4);
    elems.put(2, 6);
    PCollection<KV<Integer, Integer>> input = pipeline.apply(Create.of(elems));
    input.apply(GroupByKey.create());
    pipeline.run();
  }

}
