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

import org.apache.avro.Schema;
import org.apache.beam.runners.core.construction.SdkComponents;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.io.AvroDatumFactory;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.StringUtils;

/** A {@link CloudObjectTranslator} for {@link AvroCoder}. */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
class AvroCoderCloudObjectTranslator implements CloudObjectTranslator<AvroCoder> {
  private static final String DATUM_FACTORY_FIELD = "datum_factory";
  private static final String SCHEMA_FIELD = "schema";

  @Override
  public CloudObject toCloudObject(AvroCoder target, SdkComponents sdkComponents) {
    CloudObject base = CloudObject.forClass(AvroCoder.class);
    byte[] serializedDatumFactory =
        SerializableUtils.serializeToByteArray(target.getDatumFactory());
    Structs.addString(
        base, DATUM_FACTORY_FIELD, StringUtils.byteArrayToJsonString(serializedDatumFactory));
    Structs.addString(base, SCHEMA_FIELD, target.getSchema().toString());
    return base;
  }

  @Override
  public AvroCoder<?> fromCloudObject(CloudObject cloudObject) {
    Schema.Parser parser = new Schema.Parser();
    byte[] deserializedDatumFactory =
        StringUtils.jsonStringToByteArray(Structs.getString(cloudObject, DATUM_FACTORY_FIELD));
    String schemaString = Structs.getString(cloudObject, SCHEMA_FIELD);
    Schema schema = parser.parse(schemaString);
    AvroDatumFactory<?> datumFactory =
        (AvroDatumFactory)
            SerializableUtils.deserializeFromByteArray(
                deserializedDatumFactory, DATUM_FACTORY_FIELD);
    return AvroCoder.of(datumFactory, schema);
  }

  @Override
  public Class<? extends AvroCoder> getSupportedClass() {
    return AvroCoder.class;
  }

  @Override
  public String cloudObjectClassName() {
    return CloudObject.forClass(AvroCoder.class).getClassName();
  }
}
