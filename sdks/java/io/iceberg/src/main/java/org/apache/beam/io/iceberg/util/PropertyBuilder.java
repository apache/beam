package org.apache.beam.io.iceberg.util;

import com.google.common.collect.ImmutableMap;
import javax.annotation.Nullable;

/**
 * Convenience utility class to build immutable maps that drops attempts
 * to set null values.
 */
public class PropertyBuilder {

  ImmutableMap.Builder<String,String> builder = ImmutableMap.builder();

  public PropertyBuilder put(String key,@Nullable Object value) {
    if(value != null) {
      builder = builder.put(key,""+value);
    }
    return this;
  }

  public ImmutableMap<String,String> build() {
    return builder.build();
  }
}
