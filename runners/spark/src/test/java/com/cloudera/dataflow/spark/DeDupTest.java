package com.cloudera.dataflow.spark;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.RemoveDuplicates;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

/**
 * A test based on {@code DeDupExample} from the SDK.
 */
public class DeDupTest {

  private static final String[] LINES_ARRAY = {
      "hi there", "hello", "hi there",
      "hi", "hello"};
  private static final List<String> LINES = Arrays.asList(LINES_ARRAY);
  private static final Set<String> EXPECTED_SET =
      ImmutableSet.of("hi there", "hi", "hello");

  @Test
  public void testRun() throws Exception {
    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());
    PCollection<String> input = p.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of());
    PCollection<String> output = input.apply(RemoveDuplicates.<String>create());
    EvaluationResult res = SparkPipelineRunner.create().run(p);
    Set<String> actualSet = new HashSet<>();
    for (String s : res.get(output)) {
      actualSet.add(s);
    }

    Assert.assertEquals(String.format("Actual lines [%s] does not equal expected [%s].",
            actualSet, EXPECTED_SET), EXPECTED_SET, actualSet);
    res.close();
  }
}
