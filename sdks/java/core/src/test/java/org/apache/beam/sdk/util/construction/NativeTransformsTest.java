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
package org.apache.beam.sdk.util.construction;

import com.google.auto.service.AutoService;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.util.construction.NativeTransforms.IsNativeTransform;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link NativeTransforms}. */
@RunWith(JUnit4.class)
public class NativeTransformsTest {
  /** A test implementation of a {@link IsNativeTransform}. */
  @AutoService(IsNativeTransform.class)
  public static class TestNativeTransform implements IsNativeTransform {

    @Override
    public boolean test(RunnerApi.PTransform pTransform) {
      return "test".equals(PTransformTranslation.urnForTransformOrNull(pTransform));
    }
  }

  @Test
  public void testMatch() {
    Assert.assertTrue(
        NativeTransforms.isNative(
            RunnerApi.PTransform.newBuilder()
                .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn("test").build())
                .build()));
  }

  @Test
  public void testNoMatch() {
    Assert.assertFalse(NativeTransforms.isNative(RunnerApi.PTransform.getDefaultInstance()));
  }
}
