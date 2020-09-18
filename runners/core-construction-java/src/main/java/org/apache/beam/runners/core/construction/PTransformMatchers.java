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

import static org.apache.beam.runners.core.construction.PTransformTranslation.WRITE_FILES_TRANSFORM_URN;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformMatcher;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View.CreatePCollectionView;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.ProcessElementMethod;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link PTransformMatcher} that matches {@link PTransform PTransforms} based on the class of the
 * transform.
 *
 * <p>Once {@link PTransform PTransforms} have URNs, this will be removed and replaced with a
 * UrnPTransformMatcher.
 */
public class PTransformMatchers {

  private PTransformMatchers() {}

  /**
   * Returns a {@link PTransformMatcher} that matches a {@link PTransform} if the URN of the {@link
   * PTransform} is equal to the URN provided ot this matcher.
   */
  public static PTransformMatcher urnEqualTo(String urn) {
    return new EqualUrnPTransformMatcher(urn);
  }

  private static class EqualUrnPTransformMatcher implements PTransformMatcher {

    private final String urn;

    private EqualUrnPTransformMatcher(String urn) {
      this.urn = urn;
    }

    @Override
    public boolean matches(AppliedPTransform<?, ?, ?> application) {
      return urn.equals(PTransformTranslation.urnForTransformOrNull(application.getTransform()));
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("urn", urn).toString();
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      EqualUrnPTransformMatcher that = (EqualUrnPTransformMatcher) o;
      return urn.equals(that.urn);
    }

    @Override
    public int hashCode() {
      return Objects.hash(urn);
    }
  }

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
   * that requires stable input, as signified by {@link ProcessElementMethod#requiresStableInput()}.
   */
  public static PTransformMatcher requiresStableInputParDoSingle() {
    return new PTransformMatcher() {
      @Override
      public boolean matches(AppliedPTransform<?, ?, ?> application) {
        PTransform<?, ?> transform = application.getTransform();
        if (transform instanceof ParDo.SingleOutput) {
          DoFn<?, ?> fn = ((ParDo.SingleOutput<?, ?>) transform).getFn();
          DoFnSignature signature = DoFnSignatures.signatureForDoFn(fn);
          return signature.processElement().requiresStableInput();
        }
        return false;
      }

      @Override
      public boolean matchesDuringValidation(AppliedPTransform<?, ?, ?> application) {
        return false;
      }

      @Override
      public String toString() {
        return MoreObjects.toStringHelper("RequiresStableInputParDoSingleMatcher").toString();
      }
    };
  }

  /**
   * A {@link PTransformMatcher} that matches a {@link ParDo.MultiOutput} containing a {@link DoFn}
   * that requires stable input, as signified by {@link ProcessElementMethod#requiresStableInput()}.
   */
  public static PTransformMatcher requiresStableInputParDoMulti() {
    return new PTransformMatcher() {
      @Override
      public boolean matches(AppliedPTransform<?, ?, ?> application) {
        PTransform<?, ?> transform = application.getTransform();
        if (transform instanceof ParDo.MultiOutput) {
          DoFn<?, ?> fn = ((ParDo.MultiOutput<?, ?>) transform).getFn();
          DoFnSignature signature = DoFnSignatures.signatureForDoFn(fn);
          return signature.processElement().requiresStableInput();
        }
        return false;
      }

      @Override
      public boolean matchesDuringValidation(AppliedPTransform<?, ?, ?> application) {
        return false;
      }

      @Override
      public String toString() {
        return MoreObjects.toStringHelper("RequiresStableInputParDoMultiMatcher").toString();
      }
    };
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
   * A {@link PTransformMatcher} that matches a {@link ParDo} by URN if it has a splittable {@link
   * DoFn}.
   */
  public static PTransformMatcher splittableParDo() {
    return new PTransformMatcher() {
      @Override
      public boolean matches(AppliedPTransform<?, ?, ?> application) {
        if (PTransformTranslation.PAR_DO_TRANSFORM_URN.equals(
            PTransformTranslation.urnForTransformOrNull(application.getTransform()))) {

          try {
            return ParDoTranslation.isSplittable(application);
          } catch (IOException e) {
            throw new RuntimeException(
                String.format(
                    "Transform with URN %s could not be translated",
                    PTransformTranslation.PAR_DO_TRANSFORM_URN),
                e);
          }
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
   * A {@link PTransformMatcher} that matches a {@link ParDo.SingleOutput} containing a {@link DoFn}
   * that is splittable, as signified by {@link ProcessElementMethod#isSplittable()}.
   */
  public static PTransformMatcher splittableProcessKeyedBounded() {
    return new PTransformMatcher() {
      @Override
      public boolean matches(AppliedPTransform<?, ?, ?> application) {
        PTransform<?, ?> transform = application.getTransform();
        if (transform instanceof SplittableParDo.ProcessKeyedElements) {
          DoFn<?, ?> fn = ((SplittableParDo.ProcessKeyedElements) transform).getFn();
          DoFnSignature signature = DoFnSignatures.signatureForDoFn(fn);
          return signature.processElement().isSplittable()
              && signature.isBoundedPerElement() == IsBounded.BOUNDED;
        }
        return false;
      }

      @Override
      public String toString() {
        return MoreObjects.toStringHelper("SplittableProcessKeyedBoundedMatcher").toString();
      }
    };
  }

  /**
   * A {@link PTransformMatcher} that matches a {@link ParDo.SingleOutput} containing a {@link DoFn}
   * that is splittable, as signified by {@link ProcessElementMethod#isSplittable()}.
   */
  public static PTransformMatcher splittableProcessKeyedUnbounded() {
    return new PTransformMatcher() {
      @Override
      public boolean matches(AppliedPTransform<?, ?, ?> application) {
        PTransform<?, ?> transform = application.getTransform();
        if (transform instanceof SplittableParDo.ProcessKeyedElements) {
          DoFn<?, ?> fn = ((SplittableParDo.ProcessKeyedElements) transform).getFn();
          DoFnSignature signature = DoFnSignatures.signatureForDoFn(fn);
          return signature.processElement().isSplittable()
              && signature.isBoundedPerElement() == IsBounded.UNBOUNDED;
        }
        return false;
      }

      @Override
      public String toString() {
        return MoreObjects.toStringHelper("SplittableProcessKeyedUnboundedMatcher").toString();
      }
    };
  }

  /**
   * A {@link PTransformMatcher} that matches a {@link ParDo} transform by URN and whether it
   * contains state or timers as specified by {@link ParDoTranslation}.
   */
  public static PTransformMatcher stateOrTimerParDo() {
    return new PTransformMatcher() {
      @Override
      public boolean matches(AppliedPTransform<?, ?, ?> application) {
        if (PTransformTranslation.PAR_DO_TRANSFORM_URN.equals(
            PTransformTranslation.urnForTransformOrNull(application.getTransform()))) {

          try {
            return ParDoTranslation.usesStateOrTimers(application);
          } catch (IOException e) {
            throw new RuntimeException(
                String.format(
                    "Transform with URN %s could not be translated",
                    PTransformTranslation.PAR_DO_TRANSFORM_URN),
                e);
          }
        }
        return false;
      }

      @Override
      public String toString() {
        return MoreObjects.toStringHelper("StateOrTimerParDoMatcher").toString();
      }
    };
  }

  /**
   * A {@link PTransformMatcher} that matches a {@link ParDo.MultiOutput} containing a {@link DoFn}
   * that uses state or timers, as specified by {@link DoFnSignature#usesState()} and {@link
   * DoFnSignature#usesTimers()}.
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
    return application -> {
      if (!(application.getTransform() instanceof CreatePCollectionView)) {
        return false;
      }
      CreatePCollectionView<?, ?> createView =
          (CreatePCollectionView<?, ?>) application.getTransform();
      ViewFn<?, ?> viewFn = createView.getView().getViewFn();
      return viewFn.getClass().equals(viewFnType);
    };
  }

  /**
   * A {@link PTransformMatcher} which matches a {@link Flatten.PCollections} which consumes no
   * input {@link PCollection PCollections}.
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
   * A {@link PTransformMatcher} which matches a {@link Flatten.PCollections} which consumes a
   * single input {@link PCollection} multiple times.
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

  /**
   * A {@link PTransformMatcher} which matches {@link GroupIntoBatches} transform that allows
   * shardable states.
   */
  public static PTransformMatcher groupWithShardableStates() {
    return new PTransformMatcher() {
      @Override
      public boolean matches(AppliedPTransform<?, ?, ?> application) {
        return application.getTransform().getClass().equals(GroupIntoBatches.class);
      }

      @Override
      public boolean matchesDuringValidation(AppliedPTransform<?, ?, ?> application) {
        return false;
      }

      @Override
      public String toString() {
        return MoreObjects.toStringHelper("groupWithShardableStatesMatcher").toString();
      }
    };
  }

  public static PTransformMatcher writeWithRunnerDeterminedSharding() {
    return application -> {
      if (WRITE_FILES_TRANSFORM_URN.equals(
          PTransformTranslation.urnForTransformOrNull(application.getTransform()))) {
        try {
          return WriteFilesTranslation.isRunnerDeterminedSharding((AppliedPTransform) application);
        } catch (IOException exc) {
          throw new RuntimeException(
              String.format(
                  "Transform with URN %s failed to parse: %s",
                  WRITE_FILES_TRANSFORM_URN, application.getTransform()),
              exc);
        }
      }
      return false;
    };
  }
}
