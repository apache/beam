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
  public abstract Class<? extends DoFn> fnClass();

  public abstract ProcessElementMethod processElement();

  @Nullable
  public abstract BundleMethod startBundle();

  @Nullable
  public abstract BundleMethod finishBundle();

  @Nullable
  public abstract LifecycleMethod setup();

  @Nullable
  public abstract LifecycleMethod teardown();

  static DoFnSignature create(
      Class<? extends DoFn> fnClass,
      ProcessElementMethod processElement,
      @Nullable BundleMethod startBundle,
      @Nullable BundleMethod finishBundle,
      @Nullable LifecycleMethod setup,
      @Nullable LifecycleMethod teardown) {
    return new AutoValue_DoFnSignature(
        fnClass,
        processElement,
        startBundle,
        finishBundle,
        setup,
        teardown);
  }

  /** Describes a {@link DoFn.ProcessElement} method. */
  @AutoValue
  public abstract static class ProcessElementMethod {
    enum Parameter {
      BOUNDED_WINDOW,
      INPUT_PROVIDER,
      OUTPUT_RECEIVER
    }

    public abstract Method targetMethod();

    public abstract List<Parameter> extraParameters();

    static ProcessElementMethod create(Method targetMethod, List<Parameter> extraParameters) {
      return new AutoValue_DoFnSignature_ProcessElementMethod(
          targetMethod, Collections.unmodifiableList(extraParameters));
    }

    /** @return true if the reflected {@link DoFn} uses a Single Window. */
    public boolean usesSingleWindow() {
      return extraParameters().contains(Parameter.BOUNDED_WINDOW);
    }
  }

  /** Describes a {@link DoFn.StartBundle} or {@link DoFn.FinishBundle} method. */
  @AutoValue
  public abstract static class BundleMethod {
    public abstract Method targetMethod();

    static BundleMethod create(Method targetMethod) {
      return new AutoValue_DoFnSignature_BundleMethod(targetMethod);
    }
  }

  /** Describes a {@link DoFn.Setup} or {@link DoFn.Teardown} method. */
  @AutoValue
  public abstract static class LifecycleMethod {
    public abstract Method targetMethod();

    static LifecycleMethod create(Method targetMethod) {
      return new AutoValue_DoFnSignature_LifecycleMethod(targetMethod);
    }
  }
}
