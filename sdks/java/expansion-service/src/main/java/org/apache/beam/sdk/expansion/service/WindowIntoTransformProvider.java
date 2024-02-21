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
package org.apache.beam.sdk.expansion.service;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.construction.WindowingStrategyTranslation;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * An implementation of {@link TypedSchemaTransformProvider} for WindowInto.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@AutoService(SchemaTransformProvider.class)
public class WindowIntoTransformProvider
    extends TypedSchemaTransformProvider<WindowIntoTransformProvider.Configuration> {
  private static final String INPUT_ROWS_TAG = "input";

  protected static final String OUTPUT_ROWS_TAG = "output";

  @Override
  protected Class<Configuration> configurationClass() {
    return Configuration.class;
  }

  @Override
  protected SchemaTransform from(Configuration configuration) {
    try {
      return new WindowIntoStrategy(
          (WindowingStrategy<Row, ?>)
              WindowingStrategyTranslation.fromProto(
                  RunnerApi.WindowingStrategy.parseFrom(
                      configuration.getSerializedWindowingStrategy()),
                  null));
    } catch (IOException exn) {
      throw new RuntimeException(exn);
    }
  }

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:yaml:window_into_strategy:v1";
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.singletonList(INPUT_ROWS_TAG);
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(OUTPUT_ROWS_TAG);
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class Configuration {

    @SuppressWarnings({"AutoValueMutable", "mutable"})
    public abstract byte[] getSerializedWindowingStrategy();

    public static Builder builder() {
      return new AutoValue_WindowIntoTransformProvider_Configuration.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setSerializedWindowingStrategy(byte[] serializedWindowingStrategy);

      public abstract Configuration build();
    }
  }

  private static class WindowIntoStrategy extends SchemaTransform {

    private final WindowingStrategy<Row, ?> windowingStrategy;

    WindowIntoStrategy(WindowingStrategy<Row, ?> windowingStrategy) {
      this.windowingStrategy = windowingStrategy;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple inputTuple) {
      PCollection<Row> input = inputTuple.get(INPUT_ROWS_TAG);
      return PCollectionRowTuple.of(
          OUTPUT_ROWS_TAG,
          input
              .apply(Window.Assign.<Row>createInternal(windowingStrategy))
              .setCoder(input.getCoder()));
    }
  }
}
