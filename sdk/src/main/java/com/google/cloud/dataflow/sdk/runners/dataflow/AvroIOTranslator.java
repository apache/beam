/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.runners.dataflow;

import com.google.api.client.util.Preconditions;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.io.AvroIO;
import com.google.cloud.dataflow.sdk.io.ShardNameTemplate;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineTranslator.TransformTranslator;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineTranslator.TranslationContext;
import com.google.cloud.dataflow.sdk.util.PathValidator;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.WindowedValue;

/**
 * Avro transform support code for the Dataflow backend.
 */
public class AvroIOTranslator {

  /**
   * Implements AvroIO Read translation for the Dataflow backend.
   */
  @SuppressWarnings("rawtypes")
  public static class ReadTranslator implements TransformTranslator<AvroIO.Read.Bound> {

    @Override
    public void translate(
        AvroIO.Read.Bound transform,
        TranslationContext context) {
      translateReadHelper(transform, context);
    }

    private <T> void translateReadHelper(
        AvroIO.Read.Bound<T> transform,
        TranslationContext context) {
      if (context.getPipelineOptions().isStreaming()) {
        throw new IllegalArgumentException("AvroIO not supported in streaming mode.");
      }

      PathValidator validator = context.getPipelineOptions().getPathValidator();
      String filepattern = validator.validateInputFilePatternSupported(transform.getFilepattern());
      context.addStep(transform, "ParallelRead");
      context.addInput(PropertyNames.FORMAT, "avro");
      context.addInput(PropertyNames.FILEPATTERN, filepattern);
      context.addValueOnlyOutput(PropertyNames.OUTPUT, transform.getOutput());
      // TODO: Orderedness?
    }
  }

  /**
   * Implements AvroIO Write translation for the Dataflow backend.
   */
  @SuppressWarnings("rawtypes")
  public static class WriteTranslator implements TransformTranslator<AvroIO.Write.Bound> {

    @Override
    public void translate(
        AvroIO.Write.Bound transform,
        TranslationContext context) {
      translateWriteHelper(transform, context);
    }

    private <T> void translateWriteHelper(
        AvroIO.Write.Bound<T> transform,
        TranslationContext context) {
      PathValidator validator = context.getPipelineOptions().getPathValidator();
      String filenamePrefix = validator.validateOutputFilePrefixSupported(
          transform.getFilenamePrefix());
      context.addStep(transform, "ParallelWrite");
      context.addInput(PropertyNames.PARALLEL_INPUT, transform.getInput());

      // TODO: drop this check when server supports alternative templates.
      switch (transform.getShardTemplate()) {
        case ShardNameTemplate.INDEX_OF_MAX:
          break;  // supported by server
        case "":
          // Empty shard template allowed - forces single output.
          Preconditions.checkArgument(transform.getNumShards() <= 1,
              "Num shards must be <= 1 when using an empty sharding template");
          break;
        default:
          throw new UnsupportedOperationException("Shard template "
              + transform.getShardTemplate()
              + " not yet supported by Dataflow service");
      }

      context.addInput(PropertyNames.FORMAT, "avro");
      context.addInput(PropertyNames.FILENAME_PREFIX, filenamePrefix);
      context.addInput(PropertyNames.SHARD_NAME_TEMPLATE, transform.getShardTemplate());
      context.addInput(PropertyNames.FILENAME_SUFFIX, transform.getFilenameSuffix());

      long numShards = transform.getNumShards();
      if (numShards > 0) {
        context.addInput(PropertyNames.NUM_SHARDS, numShards);
      }

      context.addEncodingInput(
          WindowedValue.getValueOnlyCoder(
              AvroCoder.of(transform.getType(), transform.getSchema())));
    }
  }
}
