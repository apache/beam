package org.apache.beam.sdk.transforms;

import java.util.Arrays;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link MapValues} transform.
 */
@RunWith(JUnit4.class)
public class MapValuesTest {

  @SuppressWarnings({
      "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
      "unchecked"
  })
  static final KV<String, Integer>[] TABLE =
      new KV[]{
          KV.of("one", 1), KV.of("two", 2), KV.of("dup", 2)
      };

  @SuppressWarnings({
      "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
      "unchecked"
  })
  static final KV<String, Integer>[] EMPTY_TABLE = new KV[]{};

  @Rule
  public final TestPipeline p = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void testMapValues() {

    PCollection<KV<String, Integer>> input =
        p.apply(Create.of(Arrays.asList(TABLE))
            .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));

    PCollection<KV<String, Double>> output =
        input.apply(MapValues.via(Integer::doubleValue));

    PAssert.that(output)
        .containsInAnyOrder(KV.of("1", 1.0d), KV.of("2", 2.0d), KV.of("dup", 2.0d));

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMapValuesEmpty() {

    PCollection<KV<String, Integer>> input =
        p.apply(Create.of(Arrays.asList(EMPTY_TABLE))
            .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));

    PCollection<KV<String, Double>> output =
        input.apply(MapValues.via(Integer::doubleValue));

    PAssert.that(output).empty();

    p.run();
  }
}
