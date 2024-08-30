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
package org.apache.beam.runners.spark.structuredstreaming.translation;

import java.util.Collection;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.ExplainMode;
import org.apache.spark.util.Utils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link EvaluationContext} is the result of a pipeline {@link PipelineTranslator#translate
 * translation} and can be used to evaluate / run the pipeline.
 *
 * <p>However, in some cases pipeline translation involves the early evaluation of some parts of the
 * pipeline. For example, this is necessary to materialize side-inputs. The {@link
 * EvaluationContext} won't re-evaluate such datasets.
 */
@Internal
public final class EvaluationContext {
  private static final Logger LOG = LoggerFactory.getLogger(EvaluationContext.class);

  interface NamedDataset<T> {
    String name();

    @Nullable
    Dataset<WindowedValue<T>> dataset();
  }

  private final Collection<? extends NamedDataset<?>> leaves;
  private final SparkSession session;

  EvaluationContext(Collection<? extends NamedDataset<?>> leaves, SparkSession session) {
    this.leaves = leaves;
    this.session = session;
  }

  /** Trigger evaluation of all leaf datasets. */
  public void evaluate() {
    for (NamedDataset<?> ds : leaves) {
      final Dataset<?> dataset = ds.dataset();
      if (dataset == null) {
        continue;
      }
      if (LOG.isDebugEnabled()) {
        ExplainMode explainMode = ExplainMode.fromString("simple");
        String execPlan = dataset.queryExecution().explainString(explainMode);
        LOG.debug("Evaluating dataset {}:\n{}", ds.name(), execPlan);
      }
      // force evaluation using a dummy foreach action
      evaluate(ds.name(), dataset);
    }
  }

  /**
   * The purpose of this utility is to mark the evaluation of Spark actions, both during Pipeline
   * translation, when evaluation is required, and when finally evaluating the pipeline.
   */
  public static <T> void evaluate(String name, Dataset<T> ds) {
    long startMs = System.currentTimeMillis();
    try {
      // force computation using noop format
      ds.write().mode("overwrite").format("noop").save();
      LOG.info("Evaluated dataset {} in {}", name, durationSince(startMs));
    } catch (RuntimeException e) {
      LOG.error("Failed to evaluate dataset {}: {}", name, Throwables.getRootCause(e).getMessage());
      throw new RuntimeException(e);
    }
  }

  /**
   * The purpose of this utility is to mark the evaluation of Spark actions, both during Pipeline
   * translation, when evaluation is required, and when finally evaluating the pipeline.
   */
  public static <T extends @NonNull Object> T[] collect(String name, Dataset<T> ds) {
    long startMs = System.currentTimeMillis();
    try {
      T[] res = (T[]) ds.collect();
      LOG.info("Collected dataset {} in {} [size: {}]", name, durationSince(startMs), res.length);
      return res;
    } catch (Exception e) {
      LOG.error("Failed to collect dataset {}: {}", name, Throwables.getRootCause(e).getMessage());
      throw new RuntimeException(e);
    }
  }

  public SparkSession getSparkSession() {
    return session;
  }

  private static String durationSince(long startMs) {
    return Utils.msDurationToString(System.currentTimeMillis() - startMs);
  }
}
