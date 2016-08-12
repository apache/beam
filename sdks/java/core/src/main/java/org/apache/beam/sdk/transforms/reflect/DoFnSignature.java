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
package org.apache.beam.sdk.transforms.reflect;

import com.google.auto.value.AutoValue;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * Describes the signature of a {@link DoFn}, in particular, which features it uses, which extra
 * context it requires, types of the input and output elements, etc.
 *
 * <p>See <a href="https://s.apache.org/a-new-dofn">A new DoFn</a>.
 */
@AutoValue
public abstract class DoFnSignature {
  /** Class of the original {@link DoFn} from which this signature was produced. */
  public abstract Class<? extends DoFn> fnClass();

  /** Details about this {@link DoFn}'s {@link DoFn.ProcessElement} method. */
  public abstract ProcessElementMethod processElement();

  /** Details about this {@link DoFn}'s {@link DoFn.StartBundle} method. */
  @Nullable
  public abstract BundleMethod startBundle();

  /** Details about this {@link DoFn}'s {@link DoFn.FinishBundle} method. */
  @Nullable
  public abstract BundleMethod finishBundle();

  /** Details about this {@link DoFn}'s {@link DoFn.Setup} method. */
  @Nullable
  public abstract LifecycleMethod setup();

  /** Details about this {@link DoFn}'s {@link DoFn.Teardown} method. */
  @Nullable
  public abstract LifecycleMethod teardown();

  static Builder builder() {
    return new AutoValue_DoFnSignature.Builder();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setFnClass(Class<? extends DoFn> fnClass);
    abstract Builder setProcessElement(ProcessElementMethod processElement);
    abstract Builder setStartBundle(BundleMethod startBundle);
    abstract Builder setFinishBundle(BundleMethod finishBundle);
    abstract Builder setSetup(LifecycleMethod setup);
    abstract Builder setTeardown(LifecycleMethod teardown);
    abstract DoFnSignature build();
  }

  /** A method delegated to a annotated method of an underlying {@link DoFn}. */
  public interface DoFnMethod {
    /** The annotated method itself. */
    Method targetMethod();
  }

  /** A type of optional parameter of the {@link DoFn.ProcessElement} method. */
  public enum Parameter {
    BOUNDED_WINDOW,
    INPUT_PROVIDER,
    OUTPUT_RECEIVER,
  }

  /** Describes a {@link DoFn.ProcessElement} method. */
  @AutoValue
  public abstract static class ProcessElementMethod implements DoFnMethod {
    /** The annotated method itself. */
    @Override
    public abstract Method targetMethod();

    /** Types of optional parameters of the annotated method, in the order they appear. */
    public abstract List<Parameter> extraParameters();

    static ProcessElementMethod create(
        Method targetMethod,
        List<Parameter> extraParameters) {
      return new AutoValue_DoFnSignature_ProcessElementMethod(
          targetMethod, Collections.unmodifiableList(extraParameters));
    }

    /** Whether this {@link DoFn} uses a Single Window. */
    public boolean usesSingleWindow() {
      return extraParameters().contains(Parameter.BOUNDED_WINDOW);
    }
  }

  /** Describes a {@link DoFn.StartBundle} or {@link DoFn.FinishBundle} method. */
  @AutoValue
  public abstract static class BundleMethod implements DoFnMethod {
    /** The annotated method itself. */
    @Override
    public abstract Method targetMethod();

    static BundleMethod create(Method targetMethod) {
      return new AutoValue_DoFnSignature_BundleMethod(targetMethod);
    }
  }

  /** Describes a {@link DoFn.Setup} or {@link DoFn.Teardown} method. */
  @AutoValue
  public abstract static class LifecycleMethod implements DoFnMethod {
    /** The annotated method itself. */
    @Override
    public abstract Method targetMethod();

    static LifecycleMethod create(Method targetMethod) {
      return new AutoValue_DoFnSignature_LifecycleMethod(targetMethod);
    }
  }
}
