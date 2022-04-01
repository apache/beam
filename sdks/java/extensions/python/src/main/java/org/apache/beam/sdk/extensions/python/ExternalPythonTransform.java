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
package org.apache.beam.sdk.extensions.python;

import java.util.Set;
import java.util.UUID;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.runners.core.construction.External;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.grpc.v1p43p2.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;

/** Wrapper for invoking external Python transforms. */
public class ExternalPythonTransform<InputT extends PInput, OutputT extends POutput>
    extends PTransform<InputT, OutputT> {
  private final String fullyQualifiedName;
  private final Row args;
  private final Row kwargs;

  public ExternalPythonTransform(String fullyQualifiedName, Row args, Row kwargs) {
    this.fullyQualifiedName = fullyQualifiedName;
    this.args = args;
    this.kwargs = kwargs;
  }

  @Override
  public OutputT expand(InputT input) {
    int port;
    try {
      port = PythonService.findAvailablePort();
      PythonService service =
          new PythonService(
              "apache_beam.runners.portability.expansion_service_main",
              "--port",
              "" + port,
              "--fully_qualified_name_glob",
              "*");
      Schema payloadSchema =
          Schema.of(
              Schema.Field.of("constructor", Schema.FieldType.STRING),
              Schema.Field.of("args", Schema.FieldType.row(args.getSchema())),
              Schema.Field.of("kwargs", Schema.FieldType.row(kwargs.getSchema())));
      payloadSchema.setUUID(UUID.randomUUID());
      Row payloadRow =
          Row.withSchema(payloadSchema).addValues(fullyQualifiedName, args, kwargs).build();
      ExternalTransforms.ExternalConfigurationPayload payload =
          ExternalTransforms.ExternalConfigurationPayload.newBuilder()
              .setSchema(SchemaTranslation.schemaToProto(payloadSchema, true))
              .setPayload(
                  ByteString.copyFrom(
                      CoderUtils.encodeToByteArray(RowCoder.of(payloadSchema), payloadRow)))
              .build();
      try (AutoCloseable p = service.start()) {
        PythonService.waitForPort("localhost", port, 30000);
        PTransform<PInput, PCollectionTuple> transform =
            External.<PInput, Object>of(
                    "beam:transforms:python:fully_qualified_named",
                    payload.toByteArray(),
                    "localhost:" + port)
                .withMultiOutputs();
        PCollectionTuple outputs;
        if (input instanceof PCollection) {
          outputs = ((PCollection<?>) input).apply(transform);
        } else if (input instanceof PCollectionTuple) {
          outputs = ((PCollectionTuple) input).apply(transform);
        } else if (input instanceof PBegin) {
          outputs = ((PBegin) input).apply(transform);
        } else {
          throw new RuntimeException("Unhandled input type " + input.getClass());
        }
        Set<TupleTag<?>> tags = outputs.getAll().keySet();
        if (tags.size() == 1) {
          return (OutputT) outputs.get(Iterables.getOnlyElement(tags));
        } else {
          return (OutputT) outputs;
        }
      }
    } catch (RuntimeException exn) {
      throw exn;
    } catch (Exception exn) {
      throw new RuntimeException(exn);
    }
  }
}
