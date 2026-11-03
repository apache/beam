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

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

/**
 * Unit tests for {@link EvaluationContext}. The static-method tests specifically exercise the
 * {@code catch} blocks that wrap and rethrow underlying Spark failures; the {@code evaluate()}
 * tests cover the happy-path evaluation of leaf datasets end-to-end via the structured-streaming
 * translation tests, and focus here on the cache cleanup added for
 * https://github.com/apache/beam/issues/40243 (datasets cached with {@link
 * PipelineTranslator.TranslationState#cacheDataset} must be unpersisted once evaluation of all
 * leaves has finished, whether it succeeded, failed, or was stopped early).
 */
public class EvaluationContextTest {

  @Test
  public void evaluateWrapsAndRethrowsRuntimeException() {
    @SuppressWarnings("unchecked")
    Dataset<Object> ds = mock(Dataset.class);
    RuntimeException underlying = new RuntimeException("boom");
    doThrow(underlying).when(ds).write();

    RuntimeException thrown =
        assertThrows(RuntimeException.class, () -> EvaluationContext.evaluate("test-ds", ds));
    assertSame(underlying, thrown.getCause());
  }

  @Test
  public void evaluateHandlesNullExceptionMessage() {
    // Reproduces the original NPE motivation for the String.valueOf wrap: a RuntimeException
    // whose root cause carries a null message must not crash the error logger.
    @SuppressWarnings("unchecked")
    Dataset<Object> ds = mock(Dataset.class);
    RuntimeException underlying = new RuntimeException((String) null);
    doThrow(underlying).when(ds).write();

    RuntimeException thrown =
        assertThrows(RuntimeException.class, () -> EvaluationContext.evaluate("test-ds", ds));
    assertSame(underlying, thrown.getCause());
  }

  @Test
  public void collectWrapsAndRethrowsException() {
    @SuppressWarnings("unchecked")
    Dataset<Object> ds = mock(Dataset.class);
    RuntimeException underlying = new RuntimeException("boom");
    doThrow(underlying).when(ds).collect();

    RuntimeException thrown =
        assertThrows(RuntimeException.class, () -> EvaluationContext.collect("test-ds", ds));
    assertSame(underlying, thrown.getCause());
  }

  @Test
  public void collectHandlesNullExceptionMessage() {
    @SuppressWarnings("unchecked")
    Dataset<Object> ds = mock(Dataset.class);
    RuntimeException underlying = new RuntimeException((String) null);
    doThrow(underlying).when(ds).collect();

    RuntimeException thrown =
        assertThrows(RuntimeException.class, () -> EvaluationContext.collect("test-ds", ds));
    assertSame(underlying, thrown.getCause());
  }

  @Test
  public void evaluateUnpersistsCachedDatasetsAfterLeavesAreEvaluated() {
    // The per-leaf write/save mechanics (and that cached Datasets actually stop being reported
    // as cached by Spark) are exercised end-to-end by ParDoTest. This test isolates the
    // cache-cleanup control flow itself using a leaf with no dataset to evaluate, a case
    // `evaluate()` already handles by skipping it -- `org.apache.spark.sql.DataFrameWriter` is a
    // final class that Mockito cannot mock, so a leaf actually going through the write path
    // cannot be stubbed here.
    @SuppressWarnings("unchecked")
    Dataset<Object> cached = mock(Dataset.class);

    EvaluationContext ctx =
        new EvaluationContext(
            Collections.singletonList(namedDataset(null)),
            Collections.singletonList(cached),
            mock(SparkSession.class));

    ctx.evaluate();

    verify(cached).unpersist();
  }

  @Test
  public void evaluateUnpersistsCachedDatasetsEvenIfLeafEvaluationFails() {
    // A failure evaluating one leaf must not leak datasets that were cached for other, unrelated
    // parts of the pipeline: https://github.com/apache/beam/issues/40243.
    @SuppressWarnings("unchecked")
    Dataset<WindowedValue<Object>> leaf = mock(Dataset.class);
    when(leaf.write()).thenThrow(new RuntimeException("boom"));

    @SuppressWarnings("unchecked")
    Dataset<Object> cached = mock(Dataset.class);

    EvaluationContext ctx =
        new EvaluationContext(
            Collections.singletonList(namedDataset(leaf)),
            Collections.singletonList(cached),
            mock(SparkSession.class));

    assertThrows(RuntimeException.class, ctx::evaluate);

    verify(cached).unpersist();
  }

  private static EvaluationContext.NamedDataset<Object> namedDataset(
      Dataset<WindowedValue<Object>> dataset) {
    return new EvaluationContext.NamedDataset<Object>() {
      @Override
      public String name() {
        return "leaf";
      }

      @Override
      public Dataset<WindowedValue<Object>> dataset() {
        return dataset;
      }
    };
  }
}
