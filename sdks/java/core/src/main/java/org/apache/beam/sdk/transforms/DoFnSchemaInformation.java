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
package org.apache.beam.sdk.transforms;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.SchemaCoder;

/** Represents information about how a DoFn extracts schemas. */
@AutoValue
public abstract class DoFnSchemaInformation implements Serializable {
  /**
   * The schema of the @Element parameter. If the Java type does not match the input PCollection but
   * the schemas are compatible, Beam will automatically convert between the Java types.
   */
  @Nullable
  public abstract SchemaCoder<?> getElementParameterSchema();

  /** Create an instance. */
  public static DoFnSchemaInformation create() {
    return new AutoValue_DoFnSchemaInformation.Builder().build();
  }

  /** The builder object. */
  @AutoValue.Builder
  public abstract static class Builder {
    abstract Builder setElementParameterSchema(@Nullable SchemaCoder<?> schemaCoder);

    abstract DoFnSchemaInformation build();
  }

  public abstract Builder toBuilder();

  public <T> DoFnSchemaInformation withElementParameterSchema(SchemaCoder<T> schemaCoder) {
    return toBuilder().setElementParameterSchema(schemaCoder).build();
  }
}
