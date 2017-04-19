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

import com.google.common.base.MoreObjects;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.beam.sdk.runners.PTransformMatcher;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View.CreatePCollectionView;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.ProcessElementMethod;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;

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

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(EqualClassPTransformMatcher.class)
          .add("class", clazz)
          .toString();
    }
  }

  /**
   * A {@link PTransformMatcher} that matches a {@link ParDo.SingleOutput} containing a {@link DoFn}
   * that is splittable, as signified by {@link ProcessElementMethod#isSplittable()}.
   */
  public static PTransformMatcher splittableParDoSingle() {
    return new PTransformMatcher() {
      @Override
      public boolean matches(AppliedPTransform<?, ?, ?> application) {
        PTransform<?, ?> transform = application.getTransform();
        if (transform instanceof ParDo.SingleOutput) {
          DoFn<?, ?> fn = ((ParDo.SingleOutput<?, ?>) transform).getFn();
          DoFnSignature signature = DoFnSignatures.signatureForDoFn(fn);
          return signature.processElement().isSplittable();
        }
        return false;
      }

      @Override
      public String toString() {
        return MoreObjects.toStringHelper("SplittableParDoSingleMatcher").toString();
      }
    };
  }

  /**
   * A {@link PTransformMatcher} that matches a {@link ParDo.SingleOutput} containing a {@link DoFn}
   * that uses state or timers, as specified by {@link DoFnSignature#usesState()} and {@link
   * DoFnSignature#usesTimers()}.
   */
  public static PTransformMatcher stateOrTimerParDoSingle() {
    return new PTransformMatcher() {
      @Override
      public boolean matches(AppliedPTransform<?, ?, ?> application) {
        PTransform<?, ?> transform = application.getTransform();
        if (transform instanceof ParDo.SingleOutput) {
          DoFn<?, ?> fn = ((ParDo.SingleOutput<?, ?>) transform).getFn();
          DoFnSignature signature = DoFnSignatures.signatureForDoFn(fn);
          return signature.usesState() || signature.usesTimers();
        }
        return false;
      }

      @Override
      public String toString() {
        return MoreObjects.toStringHelper("StateOrTimerParDoSingleMatcher").toString();
      }
    };
  }

  /**
   * A {@link PTransformMatcher} that matches a {@link ParDo.MultiOutput} containing a {@link DoFn}
   * that is splittable, as signified by {@link ProcessElementMethod#isSplittable()}.
   */
  public static PTransformMatcher splittableParDoMulti() {
    return new PTransformMatcher() {
      @Override
      public boolean matches(AppliedPTransform<?, ?, ?> application) {
        PTransform<?, ?> transform = application.getTransform();
        if (transform instanceof ParDo.MultiOutput) {
          DoFn<?, ?> fn = ((ParDo.MultiOutput<?, ?>) transform).getFn();
          DoFnSignature signature = DoFnSignatures.signatureForDoFn(fn);
          return signature.processElement().isSplittable();
        }
        return false;
      }

      @Override
      public String toString() {
        return MoreObjects.toStringHelper("SplittableParDoMultiMatcher").toString();
      }
    };
  }

  /**
   * A {@link PTransformMatcher} that matches a {@link ParDo.MultiOutput} containing a {@link DoFn}
   * that uses state or timers, as specified by {@link DoFnSignature#usesState()} and
   * {@link DoFnSignature#usesTimers()}.
   */
  public static PTransformMatcher stateOrTimerParDoMulti() {
    return new PTransformMatcher() {
      @Override
      public boolean matches(AppliedPTransform<?, ?, ?> application) {
        PTransform<?, ?> transform = application.getTransform();
        if (transform instanceof ParDo.MultiOutput) {
          DoFn<?, ?> fn = ((ParDo.MultiOutput<?, ?>) transform).getFn();
          DoFnSignature signature = DoFnSignatures.signatureForDoFn(fn);
          return signature.usesState() || signature.usesTimers();
        }
        return false;
      }

      @Override
      public String toString() {
        return MoreObjects.toStringHelper("StateOrTimerParDoMultiMatcher").toString();
      }
    };
  }

  /**
   * A {@link PTransformMatcher} which matches a {@link ParDo.SingleOutput} or {@link
   * ParDo.MultiOutput} where the {@link DoFn} is of the provided type.
   */
  public static PTransformMatcher parDoWithFnType(final Class<? extends DoFn> fnType) {
    return new PTransformMatcher() {
      @Override
      public boolean matches(AppliedPTransform<?, ?, ?> application) {
        DoFn<?, ?> fn;
        if (application.getTransform() instanceof ParDo.SingleOutput) {
          fn = ((ParDo.SingleOutput) application.getTransform()).getFn();
        } else if (application.getTransform() instanceof ParDo.MultiOutput) {
          fn = ((ParDo.MultiOutput) application.getTransform()).getFn();
        } else {
          return false;
        }
        return fnType.equals(fn.getClass());
      }

      @Override
      public String toString() {
        return MoreObjects.toStringHelper("ParDoWithFnTypeMatcher")
            .add("fnType", fnType)
            .toString();
      }
    };
  }

  public static PTransformMatcher createViewWithViewFn(final Class<? extends ViewFn> viewFnType) {
    return new PTransformMatcher() {
      @Override
      public boolean matches(AppliedPTransform<?, ?, ?> application) {
        if (!(application.getTransform() instanceof CreatePCollectionView)) {
          return false;
        }
        CreatePCollectionView<?, ?> createView =
            (CreatePCollectionView<?, ?>) application.getTransform();
        ViewFn<Iterable<WindowedValue<?>>, ?> viewFn = createView.getView().getViewFn();
        return viewFn.getClass().equals(viewFnType);
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

      @Override
      public String toString() {
        return MoreObjects.toStringHelper("EmptyFlattenMatcher").toString();
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
          for (PValue pvalue : application.getInputs().values()) {
            boolean firstInstance = observed.add(pvalue);
            if (!firstInstance) {
              return true;
            }
          }
        }
        return false;
      }

      @Override
      public String toString() {
        return MoreObjects.toStringHelper("FlattenWithDuplicateInputsMatcher").toString();
      }
    };
  }

  public static PTransformMatcher writeWithRunnerDeterminedSharding() {
    return new PTransformMatcher() {
      @Override
      public boolean matches(AppliedPTransform<?, ?, ?> application) {
        if (application.getTransform() instanceof WriteFiles) {
          WriteFiles write = (WriteFiles) application.getTransform();
          return write.getSharding() == null && write.getNumShards() == null;
        }
        return false;
      }
    };
  }
}
