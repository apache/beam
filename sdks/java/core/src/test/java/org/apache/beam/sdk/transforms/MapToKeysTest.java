package org.apache.beam.sdk.transforms;

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

import java.util.Arrays;

/**
 * Tests for {@link MapToKeys} transform.
 */
@RunWith(JUnit4.class)
public class MapToKeysTest {

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
    public void testMapToKeys() {

        PCollection<KV<Integer, String>> input =
                p.apply(Create.of(Arrays.asList(TABLE))
                        .withCoder(KvCoder.of(BigEndianIntegerCoder.of(), StringUtf8Coder.of())));

        PCollection<Double> output =
                input.apply(MapToKeys.via(Integer::doubleValue));

        PAssert.that(output)
                .containsInAnyOrder(1.0d, 2.0d, 3.0d);

        p.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testMapToKeysEmpty() {

        PCollection<KV<Integer, String>> input =
                p.apply(Create.of(Arrays.asList(EMPTY_TABLE))
                        .withCoder(KvCoder.of(BigEndianIntegerCoder.of(), StringUtf8Coder.of())));

        PCollection<Double> output =
                input.apply(MapToKeys.via(Integer::doubleValue));

        PAssert.that(output).empty();

        p.run();
    }
}