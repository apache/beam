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
package org.apache.beam.runners.spark.structuredstreaming.translation.streaming;

import static org.apache.spark.sql.functions.col;

import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.io.streaming.UnboundedSourceDataset;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.CoderHelpers;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.construction.SplittableParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

/**
 * Translator for {@link SplittableParDo.PrimitiveUnboundedRead}.
 *
 * <p>Elements arrive in the global window with the record timestamp. Downstream windowing requires
 * an explicit {@code Window.Assign}.
 */
public class ReadUnboundedTranslator<T>
    extends TransformTranslator<PBegin, PCollection<T>, SplittableParDo.PrimitiveUnboundedRead<T>> {

  public ReadUnboundedTranslator() {
    super(0.05f);
  }

  @Override
  protected void translate(SplittableParDo.PrimitiveUnboundedRead<T> transform, Context cxt) {
    PCollection<T> output = cxt.getOutput();
    UnboundedSource<T, ?> source = transform.getSource();
    Coder<T> elementCoder = output.getCoder();

    WindowedValues.FullWindowedValueCoder<T> payloadCoder =
        WindowedValues.getFullCoder(elementCoder, GlobalWindow.Coder.INSTANCE);

    SparkStructuredStreamingPipelineOptions options =
        cxt.getOptions().as(SparkStructuredStreamingPipelineOptions.class);

    Dataset<Row> rows =
        UnboundedSourceDataset.of(
            cxt.getSparkSession(),
            source,
            payloadCoder,
            options,
            cxt.getCurrentTransform().getFullName());

    Encoder<WindowedValue<T>> encoder =
        cxt.windowedEncoder(elementCoder, GlobalWindow.Coder.INSTANCE);

    Dataset<WindowedValue<T>> dataset =
        rows.select(col(UnboundedSourceDataset.COL_PAYLOAD))
            .as(Encoders.BINARY())
            .map(new DecodePayload<>(payloadCoder), encoder);

    cxt.putDataset(output, dataset);
  }

  /** Decodes the binary payload column back into a Beam {@code WindowedValue}. */
  private static final class DecodePayload<T> implements MapFunction<byte[], WindowedValue<T>> {
    private final Coder<WindowedValue<T>> coder;

    DecodePayload(Coder<WindowedValue<T>> coder) {
      this.coder = coder;
    }

    @Override
    public WindowedValue<T> call(byte[] payload) {
      return CoderHelpers.fromByteArray(payload, coder);
    }
  }
}
