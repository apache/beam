package org.apache.beam.sdk.extensions.sql.impl.udaf;

import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.annotations.Experimental;

import java.util.logging.Logger;

/**
 * {@code PTransform}s for aggregating strings or bytes with an optional delimiter (default comma)
 */

@Experimental
public class StringAgg {

  private static final Logger LOG = Logger.getLogger(StringAgg.class.getName());

  public static class StringAggString extends CombineFn<String, String, String> {
    /* initial accumulator is an empty string */
    private final String delimiter = ",";

    @Override
    public String createAccumulator() {
      return "";
    }

    @Override
    public String addInput(String curString, String nextString) {

      if (!nextString.isEmpty()) {
        if (!curString.isEmpty()) {
          curString += this.delimiter + nextString;
        } else {
          curString = nextString;
        }
      }

      return curString;
    }

    @Override
    public String mergeAccumulators(Iterable<String> accumList) {
      String mergeString = "";
      for(String stringAccum : accumList) {
        if(!stringAccum.isEmpty()) {
          if(!mergeString.isEmpty()) {
            mergeString += this.delimiter + stringAccum;
          } else {
            mergeString = stringAccum;
          }
        }
      }

      return mergeString;
    }

    @Override
    public String extractOutput(String output) {
      return output;
    }

  }
}
