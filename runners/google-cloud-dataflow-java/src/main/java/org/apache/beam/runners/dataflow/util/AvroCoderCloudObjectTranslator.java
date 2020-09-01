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

/** A {@link CloudObjectTranslator} for {@link AvroCoder}. */
class AvroCoderCloudObjectTranslator implements CloudObjectTranslator<AvroCoder> {
  private static final String TYPE_FIELD = "type";
  private static final String SCHEMA_FIELD = "schema";

  @Override
  public CloudObject toCloudObject(AvroCoder target, SdkComponents sdkComponents) {
    CloudObject base = CloudObject.forClass(AvroCoder.class);
    Structs.addString(base, SCHEMA_FIELD, target.getSchema().toString());
    Structs.addString(base, TYPE_FIELD, target.getType().getName());
    return base;
  }

  @Override
  public AvroCoder<?> fromCloudObject(CloudObject cloudObject) {
    Schema.Parser parser = new Schema.Parser();
    String className = Structs.getString(cloudObject, TYPE_FIELD);
    String schemaString = Structs.getString(cloudObject, SCHEMA_FIELD);
    try {
      Class<?> type = Class.forName(className);
      Schema schema = parser.parse(schemaString);
      return AvroCoder.of(type, schema);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
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
