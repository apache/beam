/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.iceberg.cdc.sink;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

/** The shuffle key of one write group: a destination and one of its shards. */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class DestinationShard {

  public abstract String getDestination();

  public abstract int getShard();

  public static Builder builder() {
    return new AutoValue_DestinationShard.Builder();
  }

  public static DestinationShard of(String destination, int shard) {
    return builder().setDestination(destination).setShard(shard).build();
  }

  /** A deterministic coder, so the key can drive a {@code GroupByKey}. */
  public static Coder<DestinationShard> coder() {
    try {
      return SchemaRegistry.createDefault().getSchemaCoder(DestinationShard.class);
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException("Could not build a coder for DestinationShard.", e);
    }
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setDestination(String destination);

    public abstract Builder setShard(int shard);

    public abstract DestinationShard build();
  }
}
