package org.apache.beam.sdk.extensions.timeseries;

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.timeseries.configuration.TSConfiguration;
import org.apache.beam.sdk.extensions.timeseries.protos.TimeSeriesData;
import org.apache.beam.sdk.extensions.timeseries.protos.TimeSeriesData.Accum;
import org.apache.beam.sdk.extensions.timeseries.protos.TimeSeriesData.TSAccum;
import org.apache.beam.sdk.extensions.timeseries.protos.TimeSeriesData.TSAccumSequence;
import org.apache.beam.sdk.extensions.timeseries.protos.TimeSeriesData.TSDataPoint;
import org.apache.beam.sdk.extensions.timeseries.protos.TimeSeriesData.TSKey;
import org.apache.beam.sdk.extensions.timeseries.transforms.TSAccumToFixedWindowSeq;
import org.apache.beam.sdk.extensions.timeseries.utils.TSDatas;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test forward and backfill for Accum Sequence. Even though the library produces heart Beat values
 * there are two cases where sequences will not be full. Case 1 - The first time a key is seen is >
 * then the windowStart boundary of a window. Case 2 - The last value of a key + Time to live is <
 * then the windowStart boundary of the window. TODO : Negative tests
 */
@RunWith(JUnit4.class)
public class BeamTimeSeriesSeqFillTest {

  @Rule public transient TestPipeline p = TestPipeline.create();

  static TSKey key = TSKey.newBuilder().setMajorKey("Test").build();

  static Timestamp windowStart =
      Timestamps.fromMillis(Instant.parse("2015-01-01T00:00:00Z").toDate().getTime());

  static Timestamp windowEnd =
      Timestamps.fromMillis(Instant.parse("2015-01-01T00:00:10Z").toDate().getTime());

  static Instant midInstant = Instant.parse("2015-01-01T00:00:04Z");

  static Timestamp midTimestamp = Timestamps.fromMillis(midInstant.getMillis());

  // Create tuple tags for the value types in each collection.
  private static final TupleTag<TSAccum> tag1 = new TupleTag<TimeSeriesData.TSAccum>("main") {};

  private static final TupleTag<TimeSeriesData.TSAccum> tag2 =
      new TupleTag<TimeSeriesData.TSAccum>("manual") {};

  @Test
  public void testSequenceForwardFill() {

    TimeSeriesOptions options = p.getOptions().as(TimeSeriesOptions.class);

    options.setDownSampleDurationMillis(1000L);
    options.setTimeToLiveMillis(0L);
    options.setFillOption(TSConfiguration.BFillOptions.LAST_KNOWN_VALUE.name());
    options.setBackPadSequencedOutput(true);
    options.setForwardPadSequencedOutput(true);

    PCollection<KV<TSKey, TSAccumSequence>> seq =
        createPCollection("Create Test Collection", p)
            .apply(new TSAccumToFixedWindowSeq(Duration.standardSeconds(10)));

    seq.apply(
            "KV->V 1",
            MapElements.into(TypeDescriptor.of(TSAccumSequence.class)).via(x -> x.getValue()))
        .apply("Extract 1", ParDo.of(new TestUtils.ExtractAccumFromSequence()))
        .apply("GlobalWindow 1", Window.into(new GlobalWindows()))
        .apply(
            new TestUtils.MatcheResults(
                tag1,
                tag2,
                p.apply("Create TestAccums", Create.of(createPerfectFillAccumSequence()))
                    .apply(
                        "KV -> V 2",
                        MapElements.into(TypeDescriptor.of(TSAccumSequence.class))
                            .via(x -> x.getValue()))
                    .apply("GlobalWindow 2", Window.into(new GlobalWindows()))
                    .apply("Extract 2", ParDo.of(new TestUtils.ExtractAccumFromSequence()))));

    PAssert.that(seq).containsInAnyOrder(createPerfectFillAccumSequence());

    p.run();
  }

  @Test
  public void testSequenceBackFill() {}

  /**
   * Todo: Full description
   *
   * @param p
   * @return
   */
  public PCollection<KV<TSKey, TSAccum>> createPCollection(String name, Pipeline p) {

    return p.apply(
            name, Create.of(KV.of(key, createSimpleTestAccum(midTimestamp, false, false).build())))
        .apply(ParDo.of(new SetTimestamp()));
  }

  private KV<TSKey, TSAccumSequence> createPerfectFillAccumSequence() {

    TSAccumSequence.Builder seq = TSAccumSequence.newBuilder();

    seq.setLowerWindowBoundary(windowStart);
    seq.setUpperWindowBoundary(windowEnd);
    seq.setDuration(Durations.fromSeconds(10));

    seq.setKey(key);

    TSAccum midPoint = createSimpleTestAccum(midTimestamp, true, true).build();

    TSAccum.Builder current = null;

    for (int i = 0; i < 10; i++) {

      // Skip mid point

      if (i == 4) {
        seq.addAccums(midPoint);
      } else {

        Timestamp startTimestamp = Timestamps.add(windowStart, Durations.fromSeconds(i));

        // The first value in the sequence will not have a previous value.
        if (i == 0) {
          current = createSimpleTestAccum(startTimestamp, false, false);
        } else {
          if (i != 5) {
            current = createSimpleTestAccum(startTimestamp, true, true);
          } else {
            current = createSimpleTestAccum(startTimestamp, true, false);
          }
        }
        seq.addAccums(current.putMetadata(TSConfiguration.HEARTBEAT, "").build());
      }
    }

    return KV.of(key, seq.build());
  }

  private TSAccum.Builder createSimpleTestAccum(
      Timestamp lower, boolean setPrevious, boolean previousHB) {

    Timestamp upper = Timestamps.add(lower, Durations.fromSeconds(1));

    // The first & last value will always have timestamp set to the midPoint value as we
    // do not adjust timestamps for first and last during a HB creation
    TSAccum.Builder accum =
        TSAccum.newBuilder()
            .setLowerWindowBoundary(lower)
            .setUpperWindowBoundary(upper)
            .setDataAccum(
                Accum.newBuilder()
                    .setMaxValue(TSDatas.createData(1))
                    .setMinValue(TSDatas.createData(1))
                    .setCount(TSDatas.createData(1))
                    .setSum(TSDatas.createData(1))
                    .setFirst(
                        TSDataPoint.newBuilder()
                            .setData(TSDatas.createData(1))
                            .setTimestamp(midTimestamp)
                            .setKey(key))
                    .setLast(
                        TSDataPoint.newBuilder()
                            .setData(TSDatas.createData(1))
                            .setTimestamp(Timestamps.add(midTimestamp, Durations.fromSeconds(1)))
                            .setKey(key)));

    TSAccum.Builder previous = null;

    if (setPrevious) {
      previous =
          TSAccum.newBuilder(accum.build())
              .setLowerWindowBoundary(Timestamps.subtract(lower, Durations.fromSeconds(1)))
              .setUpperWindowBoundary(lower);

      if (previousHB) {
        previous.putMetadata(TSConfiguration.HEARTBEAT, "");
      }

      accum.setPreviousWindowValue(previous);
    }

    return accum;
  }

  /** Set a timestamp mid-point in the fixed window. */
  public static class SetTimestamp extends DoFn<KV<TSKey, TSAccum>, KV<TSKey, TSAccum>> {

    @ProcessElement
    public void process(ProcessContext c) {
      c.outputWithTimestamp(c.element(), midInstant);
    }
  }
}
