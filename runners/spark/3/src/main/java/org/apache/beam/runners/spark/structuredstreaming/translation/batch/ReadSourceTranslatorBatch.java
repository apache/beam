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
package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import java.io.IOException;
import java.util.function.Supplier;
import org.apache.beam.runners.spark.structuredstreaming.io.BoundedDatasetFactory;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.SplittableParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.SparkSession;

/**
 * Translator for a {@link SplittableParDo.PrimitiveBoundedRead} that creates a Dataset via an RDD
 * to avoid an additional serialization roundtrip.
 */
class ReadSourceTranslatorBatch<T>
    extends TransformTranslator<PBegin, PCollection<T>, SplittableParDo.PrimitiveBoundedRead<T>> {

  ReadSourceTranslatorBatch() {
    super(0.05f);
  }

  @Override
  public void translate(SplittableParDo.PrimitiveBoundedRead<T> transform, Context cxt)
      throws IOException {
    SparkSession session = cxt.getSparkSession();
    BoundedSource<T> source = transform.getSource();
    Supplier<PipelineOptions> options = cxt.getOptionsSupplier();

    Encoder<WindowedValue<T>> encoder =
        cxt.windowedEncoder(source.getOutputCoder(), GlobalWindow.Coder.INSTANCE);

    cxt.putDataset(
        cxt.getOutput(),
        BoundedDatasetFactory.createDatasetFromRDD(session, source, options, encoder),
        false);
  }
}
