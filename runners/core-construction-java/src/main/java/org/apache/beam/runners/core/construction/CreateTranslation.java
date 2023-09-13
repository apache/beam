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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.runners.core.construction.PTransformTranslation.TransformPayloadTranslator;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Create.Values;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;

@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness"
})
public class CreateTranslation implements TransformPayloadTranslator<Create.Values<?>> {

  Schema createConfigSchema =
      Schema.builder()
          .addArrayField("values", FieldType.BYTES)
          .addByteArrayField("serialized_coder")
          .build();

  @Override
  public String getUrn() {
    return PTransformTranslation.CREATE_TRANSFORM_URN;
  }

  @Override
  public @Nullable FunctionSpec translate(
      AppliedPTransform<?, ?, Values<?>> application, SdkComponents components) throws IOException {
    // Currently just returns an empty payload.
    // We can implement an actual payload of runners start using this transform.
    return FunctionSpec.newBuilder().setUrn(getUrn(application.getTransform())).build();
  }

  private byte[] toByteArray(Object object) {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bos)) {
      out.writeObject(object);
      return bos.toByteArray();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Object fromByteArray(byte[] bytes) {
    try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInputStream in = new ObjectInputStream(bis)) {
      return in.readObject();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public @Nullable Row toConfigRow(Values<?> transform) {
    List<byte[]> encodedElements = new ArrayList<>();
    transform
        .getElements()
        .forEach(
            object -> {
              encodedElements.add(toByteArray(object));
            });

    byte[] serializedCoder =
        transform.getCoder() != null ? toByteArray(transform.getCoder()) : new byte[] {};
    return Row.withSchema(createConfigSchema)
        .withFieldValue("values", encodedElements)
        .withFieldValue("serialized_coder", serializedCoder)
        .build();
  }

  @Override
  public Create.@Nullable Values<?> fromConfigRow(Row configRow) {
    Values transform =
        Create.of(
            configRow.getArray("values").stream()
                .map(bytesValue -> fromByteArray((byte[]) bytesValue))
                .collect(Collectors.toList()));
    byte[] serializedCoder = configRow.getBytes("serialized_coder");
    if (serializedCoder.length > 0) {
      Coder coder = (Coder) fromByteArray(serializedCoder);
      transform = transform.withCoder(coder);
    }
    return transform;
  }

  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class Registrar implements TransformPayloadTranslatorRegistrar {
    @Override
    public Map<? extends Class<? extends PTransform>, ? extends TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return Collections.singletonMap(Create.Values.class, new CreateTranslation());
    }
  }
}
