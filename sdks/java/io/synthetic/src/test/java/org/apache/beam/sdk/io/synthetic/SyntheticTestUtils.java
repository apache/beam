package org.apache.beam.sdk.io.synthetic;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

/** Utility functions used for testing of {@link org.apache.beam.sdk.io.synthetic}. */
public class SyntheticTestUtils {

  /** Create a PipelineOptions object from a JSON string. */
  public static <T extends SyntheticOptions> T optionsFromString(String json, Class<T> type)
      throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    T result = mapper.readValue(json, type);
    result.validate();
    return result;
  }
}
