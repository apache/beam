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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CollectionCoder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.SetCoder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.TimestampPrefixingWindowCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.io.AvroGeneratedUser;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.logicaltypes.FixedBytes;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.join.CoGbkResult.CoGbkResultCoder;
import org.apache.beam.sdk.transforms.join.CoGbkResultSchema;
import org.apache.beam.sdk.transforms.join.UnionCoder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** Tests for {@link CloudObjects}. */
@RunWith(Enclosed.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class CloudObjectsTest {
  private static final Schema TEST_SCHEMA =
      Schema.builder()
          .addBooleanField("bool")
          .addByteField("int8")
          .addInt16Field("int16")
          .addInt32Field("int32")
          .addInt64Field("int64")
          .addFloatField("float")
          .addDoubleField("double")
          .addStringField("string")
          .addArrayField("list_int32", FieldType.INT32)
          .addLogicalTypeField("fixed_bytes", FixedBytes.of(4))
          .build();

  /** Tests that all of the Default Coders are tested. */
  @RunWith(JUnit4.class)
  public static class DefaultsPresentTest {
    @Test
    public void defaultCodersAllTested() {
      Set<Class<? extends Coder>> defaultCoderTranslators =
          new DefaultCoderCloudObjectTranslatorRegistrar().classesToTranslators().keySet();
      Set<Class<? extends Coder>> testedClasses = new HashSet<>();
      for (Coder<?> tested : DefaultCoders.data()) {
        if (tested instanceof ObjectCoder || tested instanceof ArbitraryCoder) {
          testedClasses.add(CustomCoder.class);
          assertThat(defaultCoderTranslators, hasItem(CustomCoder.class));
        } else if (AvroCoder.class.isAssignableFrom(tested.getClass())) {
          testedClasses.add(AvroCoder.class);
          assertThat(defaultCoderTranslators, hasItem(AvroCoder.class));
        } else {
          testedClasses.add(tested.getClass());
          assertThat(defaultCoderTranslators, hasItem(tested.getClass()));
        }
      }
      Set<Class<? extends Coder>> missing = new HashSet<>();
      missing.addAll(defaultCoderTranslators);
      missing.removeAll(testedClasses);
      assertThat("Coders with custom serializers should all be tested", missing, emptyIterable());
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
      ImmutableList.Builder<Coder<?>> dataBuilder =
          ImmutableList.<Coder<?>>builder()
              .add(new ArbitraryCoder())
              .add(new ObjectCoder())
              .add(GlobalWindow.Coder.INSTANCE)
              .add(IntervalWindow.getCoder())
              .add(TimestampPrefixingWindowCoder.of(IntervalWindow.getCoder()))
              .add(LengthPrefixCoder.of(VarLongCoder.of()))
              .add(IterableCoder.of(VarLongCoder.of()))
              .add(KvCoder.of(VarLongCoder.of(), ByteArrayCoder.of()))
              .add(
                  WindowedValue.getFullCoder(
                      KvCoder.of(VarLongCoder.of(), ByteArrayCoder.of()),
                      IntervalWindow.getCoder()))
              .add(ByteArrayCoder.of())
              .add(VarLongCoder.of())
              .add(SerializableCoder.of(Record.class))
              .add(AvroCoder.generic(avroSchema))
              .add(AvroCoder.specific(AvroGeneratedUser.class))
              .add(AvroCoder.reflect(Record.class))
              .add(CollectionCoder.of(VarLongCoder.of()))
              .add(ListCoder.of(VarLongCoder.of()))
              .add(SetCoder.of(VarLongCoder.of()))
              .add(MapCoder.of(VarLongCoder.of(), ByteArrayCoder.of()))
              .add(NullableCoder.of(IntervalWindow.getCoder()))
              .add(TimestampedValue.TimestampedValueCoder.of(VarLongCoder.of()))
              .add(
                  UnionCoder.of(
                      ImmutableList.of(
                          VarLongCoder.of(),
                          ByteArrayCoder.of(),
                          KvCoder.of(VarLongCoder.of(), ByteArrayCoder.of()))))
              .add(
                  CoGbkResultCoder.of(
                      CoGbkResultSchema.of(
                          ImmutableList.of(new TupleTag<Long>(), new TupleTag<byte[]>())),
                      UnionCoder.of(ImmutableList.of(VarLongCoder.of(), ByteArrayCoder.of()))))
              .add(
                  SchemaCoder.of(
                      Schema.builder().build(),
                      TypeDescriptors.rows(),
                      new RowIdentity(),
                      new RowIdentity()))
              .add(
                  SchemaCoder.of(
                      TEST_SCHEMA, TypeDescriptors.rows(), new RowIdentity(), new RowIdentity()))
              .add(RowCoder.of(TEST_SCHEMA));
      for (Class<? extends Coder> atomicCoder :
          DefaultCoderCloudObjectTranslatorRegistrar.KNOWN_ATOMIC_CODERS) {
        dataBuilder.add(InstanceBuilder.ofType(atomicCoder).fromFactoryMethod("of").build());
      }
      return dataBuilder.build();
    }

    @Parameter(0)
    public Coder<?> coder;

    @Test
    public void toAndFromCloudObject() throws Exception {
      CloudObject cloudObject = CloudObjects.asCloudObject(coder, /*sdkComponents=*/ null);
      Coder<?> fromCloudObject = CloudObjects.coderFromCloudObject(cloudObject);

      assertEquals(coder.getClass(), fromCloudObject.getClass());
      assertEquals(coder, fromCloudObject);
    }

    @Test
    public void toAndFromCloudObjectWithSdkComponents() throws Exception {
      SdkComponents sdkComponents = SdkComponents.create();
      CloudObject cloudObject = CloudObjects.asCloudObject(coder, sdkComponents);
      Coder<?> fromCloudObject = CloudObjects.coderFromCloudObject(cloudObject);

      assertEquals(coder.getClass(), fromCloudObject.getClass());
      assertEquals(coder, fromCloudObject);

      checkPipelineProtoCoderIds(coder, cloudObject, sdkComponents);
    }

    private static void checkPipelineProtoCoderIds(
        Coder<?> coder, CloudObject cloudObject, SdkComponents sdkComponents) throws Exception {
      if (CloudObjects.DATAFLOW_KNOWN_CODERS.contains(coder.getClass())) {
        assertFalse(cloudObject.containsKey(PropertyNames.PIPELINE_PROTO_CODER_ID));
      } else {
        assertTrue(cloudObject.containsKey(PropertyNames.PIPELINE_PROTO_CODER_ID));
        assertEquals(
            sdkComponents.registerCoder(coder),
            ((CloudObject) cloudObject.get(PropertyNames.PIPELINE_PROTO_CODER_ID))
                .get(PropertyNames.VALUE));
      }
      List<? extends Coder<?>> expectedComponents;
      if (coder instanceof StructuredCoder) {
        expectedComponents = ((StructuredCoder) coder).getComponents();
      } else {
        expectedComponents = coder.getCoderArguments();
      }
      Object cloudComponentsObject = cloudObject.get(PropertyNames.COMPONENT_ENCODINGS);
      List<CloudObject> cloudComponents;
      if (cloudComponentsObject == null) {
        cloudComponents = Lists.newArrayList();
      } else {
        assertThat(cloudComponentsObject, instanceOf(List.class));
        cloudComponents = (List<CloudObject>) cloudComponentsObject;
      }
      assertEquals(expectedComponents.size(), cloudComponents.size());
      for (int i = 0; i < expectedComponents.size(); i++) {
        checkPipelineProtoCoderIds(
            expectedComponents.get(i), cloudComponents.get(i), sdkComponents);
      }
    }
  }

  private static class Record implements Serializable {}

  private static org.apache.avro.Schema avroSchema =
      new org.apache.avro.Schema.Parser()
          .parse(
              "{\"namespace\": \"example.avro\",\n"
                  + " \"type\": \"record\",\n"
                  + " \"name\": \"TestAvro\",\n"
                  + " \"fields\": []\n"
                  + "}");

  private static class ObjectCoder extends CustomCoder<Object> {
    @Override
    public void encode(Object value, OutputStream outStream) throws CoderException, IOException {}

    @Override
    public Object decode(InputStream inStream) throws CoderException, IOException {
      return new Object();
    }

    @Override
    public boolean equals(@Nullable Object other) {
      return other != null && getClass().equals(other.getClass());
    }

    @Override
    public int hashCode() {
      return getClass().hashCode();
    }
  }

  /** A non-custom coder with no registered translator. */
  private static class ArbitraryCoder extends StructuredCoder<Record> {
    @Override
    public void encode(Record value, OutputStream outStream) throws CoderException, IOException {}

    @Override
    public Record decode(InputStream inStream) throws CoderException, IOException {
      return new Record();
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Collections.emptyList();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {}
  }

  /** Hack to satisfy SchemaCoder.equals until BEAM-8146 is fixed. */
  private static class RowIdentity implements SerializableFunction<Row, Row> {
    @Override
    public Row apply(Row input) {
      return input;
    }

    @Override
    public int hashCode() {
      return Objects.hash(getClass());
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      return o != null && getClass() == o.getClass();
    }
  }
}
