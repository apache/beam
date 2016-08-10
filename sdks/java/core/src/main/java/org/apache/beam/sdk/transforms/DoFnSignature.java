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
package org.apache.beam.sdk.transforms;

import com.google.common.reflect.TypeToken;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Describes the signature of a {@link DoFn}, in particular, which features it uses, which extra
 * context it requires, types of the input and output elements, etc.
 *
 * <p>See <a href="https://s.apache.org/a-new-dofn">A new DoFn</a>.
 */
class DoFnSignature {
  private final Class<? extends DoFn> fnClass;
  private final TypeToken<?> inputT;
  private final TypeToken<?> outputT;
  private final ProcessElementMethod processElement;
  @Nullable private final BundleMethod startBundle;
  @Nullable private final BundleMethod finishBundle;
  @Nullable private final LifecycleMethod setup;
  @Nullable private final LifecycleMethod teardown;

  DoFnSignature(
      Class<? extends DoFn> fnClass,
      TypeToken<?> inputT,
      TypeToken<?> outputT,
      ProcessElementMethod processElement,
      @Nullable BundleMethod startBundle,
      @Nullable BundleMethod finishBundle,
      @Nullable LifecycleMethod setup,
      @Nullable LifecycleMethod teardown) {
    this.fnClass = fnClass;
    this.inputT = inputT;
    this.outputT = outputT;
    this.processElement = processElement;
    this.startBundle = startBundle;
    this.finishBundle = finishBundle;
    this.setup = setup;
    this.teardown = teardown;
  }

  public Class<? extends DoFn> getFnClass() {
    return fnClass;
  }

  public TypeToken<?> getInputT() {
    return inputT;
  }

  public TypeToken<?> getOutputT() {
    return outputT;
  }

  public ProcessElementMethod getProcessElement() {
    return processElement;
  }

  @Nullable
  public BundleMethod getStartBundle() {
    return startBundle;
  }

  @Nullable
  public BundleMethod getFinishBundle() {
    return finishBundle;
  }

  @Nullable
  public LifecycleMethod getSetup() {
    return setup;
  }

  @Nullable
  public LifecycleMethod getTeardown() {
    return teardown;
  }

  static class DoFnMethod {
    /** The relevant method in the user's class. */
    private final Method targetMethod;

    DoFnMethod(Method targetMethod) {
      this.targetMethod = targetMethod;
    }

    public Method getTargetMethod() {
      return targetMethod;
    }
  }

  /** Describes a {@link DoFn.ProcessElement} method. */
  static class ProcessElementMethod extends DoFnMethod {
    enum Parameter {
      BOUNDED_WINDOW,
      INPUT_PROVIDER,
      OUTPUT_RECEIVER
    }

    private final List<Parameter> extraParameters;

    ProcessElementMethod(Method targetMethod, List<Parameter> extraParameters) {
      super(targetMethod);
      this.extraParameters = Collections.unmodifiableList(extraParameters);
    }

    List<Parameter> getExtraParameters() {
      return extraParameters;
    }

    /** @return true if the reflected {@link DoFn} uses a Single Window. */
    public boolean usesSingleWindow() {
      return extraParameters.contains(Parameter.BOUNDED_WINDOW);
    }
  }

  /** Describes a {@link DoFn.StartBundle} or {@link DoFn.FinishBundle} method. */
  static class BundleMethod extends DoFnMethod {
    BundleMethod(Method targetMethod) {
      super(targetMethod);
    }
  }

  /** Describes a {@link DoFn.Setup} or {@link DoFn.Teardown} method. */
  static class LifecycleMethod extends DoFnMethod {
    LifecycleMethod(Method targetMethod) {
      super(targetMethod);
    }
  }
}
