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
package org.apache.beam.runners.twister2;

import edu.iu.dsc.tws.api.tset.sets.TSet;
import edu.iu.dsc.tws.api.tset.sets.batch.BatchTSet;
import edu.iu.dsc.tws.tset.TBaseGraph;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import org.apache.beam.runners.twister2.translators.Twister2BatchPipelineTranslator;
import org.apache.beam.runners.twister2.translators.Twister2PipelineTranslator;
import org.apache.beam.runners.twister2.translators.Twister2StreamPipelineTranslator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.slf4j.LoggerFactory;

/** Twister2PipelineExecutionEnvironment. */
public class Twister2PipelineExecutionEnvironment {
  private static final Logger LOG =
      Logger.getLogger(Twister2PipelineExecutionEnvironment.class.getName());

  private final Twister2PipelineOptions options;
  private Twister2TranslationContext twister2TranslationContext;

  public Twister2PipelineExecutionEnvironment(Twister2PipelineOptions options) {
    this.options = options;
    options.setTSetEnvironment(new BeamBatchTSetEnvironment());
  }

  /** translate the pipline into Twister2 TSet graph. */
  public void translate(Pipeline pipeline) {

    TranslationModeDetector detector = new TranslationModeDetector();
    pipeline.traverseTopologically(detector);

    if (detector.isStreaming()) {
      LOG.info("Found unbounded PCollection. Switching to streaming execution.");
      options.setStreaming(true);
      throw new UnsupportedOperationException(
          "Streaming is not supported currently in the Twister2 Runner");
    }

    Twister2PipelineTranslator translator;
    if (options.isStreaming()) {
      twister2TranslationContext = new Twister2StreamTranslationContext(options);
      translator = new Twister2StreamPipelineTranslator(options, twister2TranslationContext);
    } else {
      twister2TranslationContext = new Twister2BatchTranslationContext(options);
      translator =
          new Twister2BatchPipelineTranslator(
              options, (Twister2BatchTranslationContext) twister2TranslationContext);
    }

    translator.translate(pipeline);
  }

  public Map<String, BatchTSet<?>> getSideInputs() {
    return twister2TranslationContext.getSideInputDataSets();
  }

  public Set<TSet> getLeaves() {
    return twister2TranslationContext.getLeaves();
  }

  protected TBaseGraph getTSetGraph() {
    return twister2TranslationContext.getEnvironment().getGraph();
  }

  /** Traverses the Pipeline to determine if this is a streaming pipeline. */
  private static class TranslationModeDetector extends Pipeline.PipelineVisitor.Defaults {
    private static final org.slf4j.Logger LOG =
        LoggerFactory.getLogger(TranslationModeDetector.class);

    private boolean isStreaming;

    TranslationModeDetector() {
      this.isStreaming = false;
    }

    boolean isStreaming() {
      return isStreaming;
    }

    @Override
    public void visitValue(PValue value, TransformHierarchy.Node producer) {
      if (!isStreaming) {
        if (value instanceof PCollection
            && ((PCollection) value).isBounded() == PCollection.IsBounded.UNBOUNDED) {
          LOG.info(
              "Found unbounded PCollection {}. Switching to streaming execution.", value.getName());
          isStreaming = true;
        }
      }
    }
  }
}
