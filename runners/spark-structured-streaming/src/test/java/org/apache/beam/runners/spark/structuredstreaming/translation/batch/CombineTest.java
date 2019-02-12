package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import java.io.Serializable;
import org.apache.beam.runners.spark.structuredstreaming.SparkPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for beam to spark {@link org.apache.beam.sdk.transforms.Combine} translation. */
@RunWith(JUnit4.class)
public class CombineTest implements Serializable {
  private static Pipeline pipeline;

  @BeforeClass
  public static void beforeClass() {
    PipelineOptions options = PipelineOptionsFactory.create().as(SparkPipelineOptions.class);
    options.setRunner(SparkRunner.class);
    pipeline = Pipeline.create(options);
  }

  @Test
  public void testCombine() {
    PCollection<Integer> input = pipeline.apply(Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
    input.apply(Combine.globally(new Combine.CombineFn<Integer, Integer, Integer>() {

      @Override public Integer createAccumulator() {
        return 0;
      }

      @Override public Integer addInput(Integer accumulator, Integer input) {
        return accumulator + input;
      }

      @Override public Integer mergeAccumulators(Iterable<Integer> accumulators) {
        Integer result = 0;
        for (Integer value : accumulators){
          result += value;
        }
        return result;
      }

      @Override public Integer extractOutput(Integer accumulator) {
        return accumulator;
      }
    }));
    pipeline.run();
  }
}
