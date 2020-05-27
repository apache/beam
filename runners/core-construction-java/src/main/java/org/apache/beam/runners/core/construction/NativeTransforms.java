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

import com.google.auto.service.AutoService;
import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.function.Predicate;
import org.apache.beam.model.pipeline.v1.RunnerApi;

/**
 * An extension point for users to define their own native transforms for usage with specific
 * runners. This extension point enables shared libraries within the Apache Beam codebase to treat
 * the native transform as a primitive transforms that the runner implicitly understands.
 *
 * <p><b>Warning:</b>Usage of native transforms within pipelines will prevent users from migrating
 * between runners as there is no expectation that the transform will be understood by all runners.
 * Note that for some use cases this can be a way to test out a new type of transform on a limited
 * set of runners and promote its adoption as a primitive within the Apache Beam model.
 *
 * <p>Note that users are required to ensure that translation and execution for the native transform
 * is supported by their runner.
 *
 * <p>Automatic registration occurs by creating a {@link ServiceLoader} entry and a concrete
 * implementation of the {@link IsNativeTransform} interface. It is optional but recommended to use
 * one of the many build time tools such as {@link AutoService} to generate the necessary META-INF
 * files automatically.
 */
public class NativeTransforms {
  /**
   * Returns true if an only if the Runner understands this transform and can handle it directly.
   */
  public static boolean isNative(RunnerApi.PTransform pTransform) {
    // TODO(BEAM-10109) Use default (context) classloader.
    Iterator<IsNativeTransform> matchers =
        ServiceLoader.load(IsNativeTransform.class, NativeTransforms.class.getClassLoader())
            .iterator();
    while (matchers.hasNext()) {
      if (matchers.next().test(pTransform)) {
        return true;
      }
    }
    return false;
  }

  /** A predicate which returns true if and only if the transform is a native transform. */
  public interface IsNativeTransform extends Predicate<RunnerApi.PTransform> {
    @Override
    boolean test(RunnerApi.PTransform pTransform);
  }
}
