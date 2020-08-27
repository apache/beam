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
package org.apache.beam.runners.direct;

import java.util.IdentityHashMap;
import java.util.Map;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.IllegalMutationException;
import org.apache.beam.sdk.util.MutationDetector;
import org.apache.beam.sdk.util.MutationDetectors;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * {@link ModelEnforcement} that enforces elements are not modified over the course of processing an
 * element.
 */
class ImmutabilityEnforcementFactory implements ModelEnforcementFactory {
  public static ModelEnforcementFactory create() {
    return new ImmutabilityEnforcementFactory();
  }

  @Override
  public <T> ModelEnforcement<T> forBundle(
      CommittedBundle<T> input, AppliedPTransform<?, ?, ?> consumer) {
    if (isReadTransform(consumer)) {
      return NoopReadEnforcement.INSTANCE;
    }
    return new ImmutabilityCheckingEnforcement<>(input, consumer);
  }

  static boolean isReadTransform(AppliedPTransform<?, ?, ?> consumer) {
    IsReadVisitor visitor = new IsReadVisitor(consumer.getTransform());
    consumer.getPipeline().traverseTopologically(visitor);
    return visitor.isRead();
  }

  private static class IsReadVisitor extends PipelineVisitor.Defaults {
    private final PTransform<?, ?> transform;
    private boolean isRead;
    private boolean isInsideRead;

    private IsReadVisitor(PTransform<?, ?> transform) {
      this.transform = transform;
    }

    @Override
    public CompositeBehavior enterCompositeTransform(Node node) {
      if (node.getTransform() instanceof Read.Bounded
          || node.getTransform() instanceof Read.Unbounded) {
        isInsideRead = true;
      }
      if (isInsideRead && node.getTransform() == transform) {
        isRead = true;
      }
      return CompositeBehavior.ENTER_TRANSFORM;
    }

    @Override
    public void leaveCompositeTransform(Node node) {
      if (node.getTransform() instanceof Read.Bounded
          || node.getTransform() instanceof Read.Unbounded) {
        isInsideRead = false;
      }
    }

    @Override
    public void visitPrimitiveTransform(Node node) {
      if (isInsideRead && node.getTransform() == transform) {
        isRead = true;
      }
    }

    private boolean isRead() {
      return isRead;
    }
  }

  private static class NoopReadEnforcement<T> extends AbstractModelEnforcement<T> {
    private static final NoopReadEnforcement INSTANCE = new NoopReadEnforcement<>();
  }

  private static class ImmutabilityCheckingEnforcement<T> extends AbstractModelEnforcement<T> {
    private final AppliedPTransform<?, ?, ?> transform;
    private final Map<WindowedValue<T>, MutationDetector> mutationElements;
    private final Coder<T> coder;

    private ImmutabilityCheckingEnforcement(
        CommittedBundle<T> input, AppliedPTransform<?, ?, ?> transform) {
      this.transform = transform;
      coder = input.getPCollection().getCoder();
      mutationElements = new IdentityHashMap<>();
    }

    @Override
    public void beforeElement(WindowedValue<T> element) {
      try {
        mutationElements.put(
            element, MutationDetectors.forValueWithCoder(element.getValue(), coder));
      } catch (CoderException e) {
        throw UserCodeException.wrap(e);
      }
    }

    @Override
    public void afterElement(WindowedValue<T> element) {
      verifyUnmodified(mutationElements.get(element));
    }

    @Override
    public void afterFinish(
        CommittedBundle<T> input,
        TransformResult<T> result,
        Iterable<? extends CommittedBundle<?>> outputs) {
      for (MutationDetector detector : mutationElements.values()) {
        verifyUnmodified(detector);
      }
    }

    private void verifyUnmodified(MutationDetector detector) {
      try {
        detector.verifyUnmodified();
      } catch (IllegalMutationException e) {
        throw new IllegalMutationException(
            String.format(
                "PTransform %s illegaly mutated value %s of class %s."
                    + " Input values must not be mutated in any way.",
                transform.getFullName(), e.getSavedValue(), e.getSavedValue().getClass()),
            e.getSavedValue(),
            e.getNewValue());
      }
    }
  }
}
