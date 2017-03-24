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
package org.apache.beam.runners.core.construction;

import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.io.Write;
import org.apache.beam.sdk.runners.PTransformMatcher;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.ProcessElementMethod;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TaggedPValue;

/**
 * A {@link PTransformMatcher} that matches {@link PTransform PTransforms} based on the class of the
 * transform.
 *
 * <p>Once {@link PTransform PTransforms} have URNs, this will be removed and replaced with a
 * UrnPTransformMatcher.
 */
@Experimental(Kind.CORE_RUNNERS_ONLY)
public class PTransformMatchers {
  private PTransformMatchers() {}

  /**
   * Returns a {@link PTransformMatcher} that matches a {@link PTransform} if the class of the
   * {@link PTransform} is equal to the {@link Class} provided ot this matcher.
   * @param clazz
   * @return
   */
  public static PTransformMatcher classEqualTo(Class<? extends PTransform> clazz) {
    return new EqualClassPTransformMatcher(clazz);
  }

  private static class EqualClassPTransformMatcher implements PTransformMatcher {
    private final Class<? extends PTransform> clazz;

    private EqualClassPTransformMatcher(Class<? extends PTransform> clazz) {
      this.clazz = clazz;
    }

    @Override
    public boolean matches(AppliedPTransform<?, ?, ?> application) {
      return application.getTransform().getClass().equals(clazz);
    }
  }

  /**
   * A {@link PTransformMatcher} that matches a {@link ParDo.Bound} containing a {@link DoFn} that
   * is splittable, as signified by {@link ProcessElementMethod#isSplittable()}.
   */
  public static PTransformMatcher splittableParDoSingle() {
    return new PTransformMatcher() {
      @Override
      public boolean matches(AppliedPTransform<?, ?, ?> application) {
        PTransform<?, ?> transform = application.getTransform();
        if (transform instanceof ParDo.Bound) {
          DoFn<?, ?> fn = ((ParDo.Bound<?, ?>) transform).getFn();
          DoFnSignature signature = DoFnSignatures.signatureForDoFn(fn);
          return signature.processElement().isSplittable();
        }
        return false;
      }
    };
  }

  /**
   * A {@link PTransformMatcher} that matches a {@link ParDo.Bound} containing a {@link DoFn} that
   * uses state or timers, as specified by {@link DoFnSignature#usesState()} and
   * {@link DoFnSignature#usesTimers()}.
   */
  public static PTransformMatcher stateOrTimerParDoSingle() {
    return new PTransformMatcher() {
      @Override
      public boolean matches(AppliedPTransform<?, ?, ?> application) {
        PTransform<?, ?> transform = application.getTransform();
        if (transform instanceof ParDo.Bound) {
          DoFn<?, ?> fn = ((ParDo.Bound<?, ?>) transform).getFn();
          DoFnSignature signature = DoFnSignatures.signatureForDoFn(fn);
          return signature.usesState() || signature.usesTimers();
        }
        return false;
      }
    };
  }

  /**
   * A {@link PTransformMatcher} that matches a {@link ParDo.BoundMulti} containing a {@link DoFn}
   * that is splittable, as signified by {@link ProcessElementMethod#isSplittable()}.
   */
  public static PTransformMatcher splittableParDoMulti() {
    return new PTransformMatcher() {
      @Override
      public boolean matches(AppliedPTransform<?, ?, ?> application) {
        PTransform<?, ?> transform = application.getTransform();
        if (transform instanceof ParDo.BoundMulti) {
          DoFn<?, ?> fn = ((ParDo.BoundMulti<?, ?>) transform).getFn();
          DoFnSignature signature = DoFnSignatures.signatureForDoFn(fn);
          return signature.processElement().isSplittable();
        }
        return false;
      }
    };
  }

  /**
   * A {@link PTransformMatcher} that matches a {@link ParDo.BoundMulti} containing a {@link DoFn}
   * that uses state or timers, as specified by {@link DoFnSignature#usesState()} and
   * {@link DoFnSignature#usesTimers()}.
   */
  public static PTransformMatcher stateOrTimerParDoMulti() {
    return new PTransformMatcher() {
      @Override
      public boolean matches(AppliedPTransform<?, ?, ?> application) {
        PTransform<?, ?> transform = application.getTransform();
        if (transform instanceof ParDo.BoundMulti) {
          DoFn<?, ?> fn = ((ParDo.BoundMulti<?, ?>) transform).getFn();
          DoFnSignature signature = DoFnSignatures.signatureForDoFn(fn);
          return signature.usesState() || signature.usesTimers();
        }
        return false;
      }
    };
  }

  /**
   * A {@link PTransformMatcher} which matches a {@link ParDo.Bound} or {@link ParDo.BoundMulti}
   * where the {@link DoFn} is of the provided type.
   */
  public static PTransformMatcher parDoWithFnType(final Class<? extends DoFn> fnType) {
    return new PTransformMatcher() {
      @Override
      public boolean matches(AppliedPTransform<?, ?, ?> application) {
        DoFn<?, ?> fn;
        if (application.getTransform() instanceof ParDo.Bound) {
          fn = ((ParDo.Bound) application.getTransform()).getFn();
        } else if (application.getTransform() instanceof ParDo.BoundMulti) {
          fn = ((ParDo.BoundMulti) application.getTransform()).getFn();
        } else {
          return false;
        }
        return fnType.equals(fn.getClass());
      }
    };
  }

  /**
   * A {@link PTransformMatcher} which matches a {@link Flatten.PCollections} which
   * consumes no input {@link PCollection PCollections}.
   */
  public static PTransformMatcher emptyFlatten() {
    return new PTransformMatcher() {
      @Override
      public boolean matches(AppliedPTransform<?, ?, ?> application) {
        return (application.getTransform() instanceof Flatten.PCollections)
            && application.getInputs().isEmpty();
      }
    };
  }

  /**
   * A {@link PTransformMatcher} which matches a {@link Flatten.PCollections} which
   * consumes a single input {@link PCollection} multiple times.
   */
  public static PTransformMatcher flattenWithDuplicateInputs() {
    return new PTransformMatcher() {
      @Override
      public boolean matches(AppliedPTransform<?, ?, ?> application) {
        if (application.getTransform() instanceof Flatten.PCollections) {
          Set<PValue> observed = new HashSet<>();
          for (TaggedPValue pvalue : application.getInputs()) {
            boolean firstInstance = observed.add(pvalue.getValue());
            if (!firstInstance) {
              return true;
            }
          }
        }
        return false;
      }
    };
  }

  public static PTransformMatcher writeWithRunnerDeterminedSharding() {
    return new PTransformMatcher() {
      @Override
      public boolean matches(AppliedPTransform<?, ?, ?> application) {
        if (application.getTransform() instanceof Write) {
          return ((Write) application.getTransform()).getSharding() == null;
        }
        return false;
      }
    };
  }
}
