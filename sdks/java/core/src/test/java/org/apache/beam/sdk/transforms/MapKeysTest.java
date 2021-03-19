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
 * Tests for {@link MapKeys} transform.
 */
@RunWith(JUnit4.class)
public class MapKeysTest {

  @SuppressWarnings({
      "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
      "unchecked"
  })
  static final KV<Integer, String>[] TABLE =
      new KV[]{
          KV.of(1, "one"), KV.of(2, "none"), KV.of(3, "none")
      };

  @SuppressWarnings({
      "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
      "unchecked"
  })
  static final KV<Integer, String>[] EMPTY_TABLE = new KV[]{};

  @Rule
  public final TestPipeline p = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void testMapKeys() {

    PCollection<KV<Integer, String>> input =
        p.apply(Create.of(Arrays.asList(TABLE))
            .withCoder(KvCoder.of(BigEndianIntegerCoder.of(), StringUtf8Coder.of())));

    PCollection<KV<Double, String>> output =
        input.apply(MapKeys.via(Integer::doubleValue));

    PAssert.that(output)
        .containsInAnyOrder(KV.of(1.0d, "one"), KV.of(2.0d, "none"), KV.of(3.0d, "none"));

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMapKeysEmpty() {

    PCollection<KV<Integer, String>> input =
        p.apply(Create.of(Arrays.asList(EMPTY_TABLE))
            .withCoder(KvCoder.of(BigEndianIntegerCoder.of(), StringUtf8Coder.of())));

    PCollection<KV<Double, String>> output =
        input.apply(MapKeys.via(Integer::doubleValue));

    PAssert.that(output).empty();

    p.run();
  }
}
