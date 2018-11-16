package org.apache.beam.sdk.extensions.timeseries;

import com.google.protobuf.util.Timestamps;
import java.util.*;
import org.apache.beam.sdk.extensions.timeseries.protos.TimeSeriesData;
import org.apache.beam.sdk.extensions.timeseries.protos.TimeSeriesData.TSAccum;
import org.apache.beam.sdk.extensions.timeseries.utils.TSAccums;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.*;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utils for Timeseries tests. */
public class TestUtils {

  private static final Logger LOG = LoggerFactory.getLogger(TestUtils.class);

  public static class MatcheResults
      extends PTransform<PCollection<KV<String, TimeSeriesData.TSAccum>>, PDone> {

    TupleTag<TimeSeriesData.TSAccum> tag1;

    TupleTag<TimeSeriesData.TSAccum> tag2;

    PCollection<KV<String, TimeSeriesData.TSAccum>> manualCreatedAccums;

    public MatcheResults(
        TupleTag<TimeSeriesData.TSAccum> tag1,
        TupleTag<TimeSeriesData.TSAccum> tag2,
        PCollection<KV<String, TimeSeriesData.TSAccum>> manualCreatedAccums) {
      this.tag1 = tag1;
      this.tag2 = tag2;
      this.manualCreatedAccums = manualCreatedAccums;
    }

    @Override
    public PDone expand(PCollection<KV<String, TimeSeriesData.TSAccum>> input) {

      // Merge collection values into a CoGbkResult collection.
      PCollection<KV<String, CoGbkResult>> coGbkResultCollection =
          KeyedPCollectionTuple.of(tag1, input)
              .and(tag2, manualCreatedAccums)
              .apply(CoGroupByKey.<String>create());

      coGbkResultCollection.apply(ParDo.of(new Match(tag1, tag2)));

      return PDone.in(input.getPipeline());
    }

    /** Use join to isolate missing items from Manual Test set and pipeline output. */
    private static class Match extends DoFn<KV<String, CoGbkResult>, String> {

      TupleTag<TimeSeriesData.TSAccum> pipelineOutput;

      TupleTag<TimeSeriesData.TSAccum> manualDataset;

      public Match(TupleTag<TimeSeriesData.TSAccum> tag1, TupleTag<TimeSeriesData.TSAccum> tag2) {
        this.pipelineOutput = tag1;
        this.manualDataset = tag2;
      }

      @ProcessElement
      public void processElement(ProcessContext c, BoundedWindow w) {

        KV<String, CoGbkResult> e = c.element();

        for (TimeSeriesData.TSAccum accum : e.getValue().getAll(manualDataset)) {

          Iterator<TimeSeriesData.TSAccum> pt2Val = e.getValue().getAll(pipelineOutput).iterator();

          if (pt2Val.hasNext()) {
            LOG.info(String.format(" Match found for Manual Dataset Item: %s ", e.getKey()));
            // Deep compare
            TimeSeriesData.TSAccum tsAccum2 = pt2Val.next();
            if (!accum.toByteString().equals(tsAccum2.toByteString())) {

              misMatchFoundThrowError(
                  String.format(
                      " Deep check failed with key %s manual value as accum 1 and pipeline value as accum 2 \n %s ",
                      e.getKey(), TSAccums.debugDetectOutputDiffBetweenTwoAccums(accum, tsAccum2)));
            }

            // If match found then no need to check for pipeline object
            return;

          } else {
            misMatchFoundThrowError(
                String.format(
                    " No match found for Manual Dataset Item: %s  window %s Accum %s",
                    e.getKey(), w, accum.toString()));
          }
        }
        // We will only be hear if there was no Manual pipeline option
        for (TimeSeriesData.TSAccum accum : e.getValue().getAll(pipelineOutput)) {
          misMatchFoundThrowError(
              String.format(
                  " No match found for Pipline Item: %s window %s Accum %s",
                  e.getKey(), w, accum.toString()));
        }
      }

      private void misMatchFoundThrowError(String error) throws IllegalStateException {
        throw new IllegalStateException(error);
      }
    }
  }

  public static Map<String, List<TimeSeriesData.TSAccum>> generateKeyMap(
      List<TimeSeriesData.TSAccum> accums) {

    Map<String, List<TimeSeriesData.TSAccum>> keyMap = new HashMap<>();

    // Seperate all keys
    for (TimeSeriesData.TSAccum accum : accums) {
      String key = TSAccums.getTSAccumMajorMinorKeyAsString(accum);

      if (keyMap.containsKey(key)) {

        keyMap.get(key).add(accum);

      } else {
        keyMap.put(key, new ArrayList<>());
        keyMap.get(key).add(accum);
      }
    }

    return keyMap;
  }

  public static class ExtractAccumFromSequence
      extends DoFn<TimeSeriesData.TSAccumSequence, KV<String, TimeSeriesData.TSAccum>> {

    @ProcessElement
    public void process(ProcessContext c) {
      for (TSAccum accum : c.element().getAccumsList()) {
        c.output(KV.of(TSAccums.getTSAccumKeyWithPrettyTimeBoundary(accum), accum));
      }
    }
  }

  /**
   * Produce Timestamped values from TSMultiVariateDataPoint.
   *
   * @param values
   * @return
   */
  public static List<TimestampedValue<TimeSeriesData.TSMultiVariateDataPoint>>
      convertTSMultiVariateDataPointToTimeStampedElement(
          List<TimeSeriesData.TSMultiVariateDataPoint> values) {
    List<TimestampedValue<TimeSeriesData.TSMultiVariateDataPoint>> output = new ArrayList<>();
    for (TimeSeriesData.TSMultiVariateDataPoint value : values) {
      output.add(
          TimestampedValue.of(
              value, new Instant().withMillis(Timestamps.toMillis(value.getTimestamp()))));
    }
    return output;
  }
}
