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

import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TranslationContext;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.RowHelpers;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.WindowingHelpers;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * By default Combine.globally is translated as a composite transform that does a Pardo (to key the input PCollection with Void keys and then a Combine.PerKey transform.
 * The problem is that Combine.PerKey uses spark GroupByKey which is not very performant due to shuffle. So we add a special
 * CombineGloballyTranslator that does not need GroupByKey
 */
class CombineGloballyTranslatorBatch<InputT, AccumT, OutputT>
    implements TransformTranslator<
        PTransform<PCollection<InputT>, PCollection<OutputT>>> {

  @Override public void translateTransform(
      PTransform<PCollection<InputT>, PCollection<OutputT>> transform, TranslationContext context) {

    Combine.Globally combineTransform = (Combine.Globally) transform;
    @SuppressWarnings("unchecked")
    final PCollection<InputT> input = (PCollection<InputT>) context.getInput();
    @SuppressWarnings("unchecked")
    final PCollection<OutputT> output = (PCollection<OutputT>) context.getOutput();
    @SuppressWarnings("unchecked")
    final Combine.CombineFn<InputT, AccumT, OutputT> combineFn =
        (Combine.CombineFn<InputT, AccumT, OutputT>) combineTransform.getFn();

    Dataset<WindowedValue<InputT>> inputDataset = context.getDataset(input);

    System.out.println("****** inputDataset ******" + inputDataset.schema());

    Dataset<InputT> unWindowedDataset =
        inputDataset.map(
            WindowingHelpers.unwindowMapFunction(), EncoderHelpers.genericEncoder());

    System.out.println("****** unWindowedDataset ******" + unWindowedDataset.schema());

    Dataset<Row> combinedRowDataset = unWindowedDataset
        .agg(new AggregatorCombinerGlobally<>(combineFn).toColumn());

    System.out.println("*****combinedRowDataset*******" + combinedRowDataset.schema());

    Dataset<OutputT> combinedDataset = combinedRowDataset
        .map(RowHelpers.extractObjectFromRowMapFunction(), EncoderHelpers.genericEncoder());

    System.out.println("****** combinedDataset ******" + combinedDataset.schema());

    // Window the result into global window.
    Dataset<WindowedValue<OutputT>> outputDataset = combinedDataset
        .map(WindowingHelpers.windowMapFunction(), EncoderHelpers.windowedValueEncoder());

    System.out.println("****** outputDataset ******" + outputDataset.schema());

    context.putDataset(output, outputDataset);
  }
}
