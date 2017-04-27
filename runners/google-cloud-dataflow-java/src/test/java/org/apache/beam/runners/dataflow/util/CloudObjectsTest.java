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

package org.apache.beam.runners.dataflow.util;

import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.util.CloudObject;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.apache.beam.sdk.util.Serializer;
import org.apache.beam.sdk.util.WindowedValue;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests for {@link CloudObjects}.
 */
@RunWith(Enclosed.class)
public class CloudObjectsTest {
  /**
   * Tests that all of the Default Coders are tested.
   */
  @RunWith(JUnit4.class)
  public static class DefaultsPresentTest {
    @Test
    public void defaultCodersAllTested() {
      Set<Class<? extends Coder>> defaultCoderTranslators =
          new DefaultCoderCloudObjectTranslatorRegistrar().classesToTranslators().keySet();
      Set<Class<? extends Coder>> testedClasses = new HashSet<>();
      for (Coder<?> tested : DefaultCoders.data()) {
        if (tested instanceof ObjectCoder) {
          testedClasses.add(CustomCoder.class);
          assertThat(defaultCoderTranslators, hasItem(CustomCoder.class));
        } else {
          testedClasses.add(tested.getClass());
          assertThat(defaultCoderTranslators, hasItem(tested.getClass()));
        }
      }
      Set<Class<? extends Coder>> missing = new HashSet<>();
      missing.addAll(defaultCoderTranslators);
      missing.removeAll(testedClasses);
      assertThat(missing, emptyIterable());
    }

    @Test
    public void defaultCodersIncludesCustomCoder() {
      Set<Class<? extends Coder>> defaultCoders =
          new DefaultCoderCloudObjectTranslatorRegistrar().classesToTranslators().keySet();
      assertThat(defaultCoders, hasItem(CustomCoder.class));
    }
  }


  /**
   * Tests that all of the registered coders in {@link DefaultCoderCloudObjectTranslatorRegistrar}
   * can be serialized and deserialized with {@link CloudObjects}.
   */
  @RunWith(Parameterized.class)
  public static class DefaultCoders {
    @Parameters(name = "{index}: {0}")
    public static Iterable<Coder<?>> data() {
      Builder<Coder<?>> dataBuilder =
          ImmutableList.<Coder<?>>builder()
              .add(new ObjectCoder())
              .add(GlobalWindow.Coder.INSTANCE)
              .add(IntervalWindow.getCoder())
              .add(LengthPrefixCoder.of(VarLongCoder.of()))
              .add(IterableCoder.of(VarLongCoder.of()))
              .add(KvCoder.of(VarLongCoder.of(), ByteArrayCoder.of()))
              .add(
                  WindowedValue.getFullCoder(
                      KvCoder.of(VarLongCoder.of(), ByteArrayCoder.of()),
                      IntervalWindow.getCoder()))
              .add(VarLongCoder.of())
              .add(ByteArrayCoder.of());
      for (Class<? extends Coder> atomicCoder :
          DefaultCoderCloudObjectTranslatorRegistrar.KNOWN_ATOMIC_CODERS) {
        dataBuilder.add(InstanceBuilder.ofType(atomicCoder).fromFactoryMethod("of").build());
      }
      return dataBuilder
          .build();
    }

    @Parameter(0) public Coder<?> coder;

    @Test
    public void toAndFromCloudObject() throws Exception {
      CloudObject cloudObject = CloudObjects.asCloudObject(coder);
      Coder<?> reconstructed = Serializer.deserialize(cloudObject, Coder.class);
      Coder<?> fromCloudObject = CloudObjects.coderFromCloudObject(cloudObject);

      assertEquals(coder.getClass(), reconstructed.getClass());
      assertEquals(coder.getClass(), fromCloudObject.getClass());
      assertEquals(coder, reconstructed);
      assertEquals(coder, fromCloudObject);
    }
  }

  private static class ObjectCoder extends CustomCoder<Object> {
    @Override
    public void encode(Object value, OutputStream outStream, Context context)
        throws CoderException, IOException {
    }

    @Override
    public Object decode(InputStream inStream, Context context)
        throws CoderException, IOException {
      return new Object();
    }

    @Override
    public boolean equals(Object other) {
      return other != null && getClass().equals(other.getClass());
    }

    @Override
    public int hashCode() {
      return getClass().hashCode();
    }
  }
}
