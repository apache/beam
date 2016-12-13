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
package org.apache.beam.sdk.testing;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * A {@link TestRule} that verifies the following assumptions are met upon running a test that
 * involves a {@link TestPipeline}:
 * <ul>
 *   <li>PAssert is not added to a pipeline that has already been run.
 *   <li>The pipeline is run after the PAssert is added.
 * </ul>
 */
public class TestPipelineRule extends TestPipeline implements TestRule {

  /**
   * A visitor that traverses a pipeline in search of a
   * {@link org.apache.beam.sdk.testing.PAssert.GroupThenAssert} node.
   */
  private static class PAssertDetector extends PipelineVisitor.Defaults {

    boolean hasPAssert = false;

    private boolean isPAssertNode(final TransformHierarchy.Node node) {
      return
          node.getTransform() instanceof PAssert.GroupThenAssert
              || node.getTransform() instanceof PAssert.GroupThenAssertForSingleton
              || node.getTransform() instanceof PAssert.OneSideInputAssert;
    }

    @Override
    public void leaveCompositeTransform(final TransformHierarchy.Node node) {
      hasPAssert = hasPAssert || isPAssertNode(node);
    }
  }

  /**
   * An exception thrown in case an invocation of {@link Pipeline#run()} was performed before any
   * {@link PAssert} assertions were set up.
   */
  public static class PipelineRunBeforePAssertException extends RuntimeException {

    PipelineRunBeforePAssertException(final String msg) {
      super(msg);
    }
  }

  /**
   * An exception thrown in case a test finishes without invoking {@link Pipeline#run()}.
   */
  public static class PipelineRunMissingException extends RuntimeException {

    PipelineRunMissingException(final String msg) {
      super(msg);
    }
  }

  private PipelineResult pipelineResult;

  public TestPipelineRule(final PipelineOptions pipelineOptions) {
    super(PipelineRunner.fromOptions(pipelineOptions), pipelineOptions);
  }

  public Statement apply(final Statement base, final Description description) {
    return new Statement() {

      private void verifyPipelineRan() {
        if (hasPAssert(TestPipelineRule.this) && pipelineResult == null) {
          throw new PipelineRunMissingException(
              "The test has finished without running the pipeline, resulting in the defined "
                  + "PAssert assertions not being executed.");
        }
      }

      @Override
      public void evaluate() throws Throwable {
        base.evaluate();
        verifyPipelineRan();
      }

    };
  }

  private boolean hasPAssert(final TestPipeline pipeline) {
    final PAssertDetector pAssertDetector = new PAssertDetector();
    pipeline.traverseTopologically(pAssertDetector);
    return pAssertDetector.hasPAssert;
  }

  @Override
  public PipelineResult run() {
    if (!hasPAssert(this)) {
      throw new PipelineRunBeforePAssertException("No PAssert assertions found for pipeline.");
    }
    pipelineResult = super.run();
    return pipelineResult;
  }
}
