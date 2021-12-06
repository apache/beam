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
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.AvroGenericCoder;
import org.apache.beam.sdk.coders.AvroReflectCoder;
import org.apache.beam.sdk.coders.AvroSpecificCoder;

/** A {@link CloudObjectTranslator} for {@link AvroCoder}. */
@SuppressWarnings({
  "rawtypes" // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
})
class AvroCoderCloudObjectTranslator<T extends AvroCoder> implements CloudObjectTranslator<T> {

  public interface AvroCoderFactory<T extends AvroCoder> {
    T apply(Class<?> type, Schema schema);
  }

  public static AvroCoderCloudObjectTranslator<AvroGenericCoder> generic() {
    return new AvroCoderCloudObjectTranslator<>(
        AvroGenericCoder.class, (t, s) -> new AvroGenericCoder(s));
  }

  public static AvroCoderCloudObjectTranslator<AvroSpecificCoder> specific() {
    return new AvroCoderCloudObjectTranslator<>(AvroSpecificCoder.class, AvroSpecificCoder::new);
  }

  public static AvroCoderCloudObjectTranslator<AvroReflectCoder> reflect() {
    return new AvroCoderCloudObjectTranslator<>(AvroReflectCoder.class, AvroReflectCoder::new);
  }

  private static final String TYPE_FIELD = "type";
  private static final String SCHEMA_FIELD = "schema";

  private final Class<T> coderClass;
  private final AvroCoderFactory<T> factory;

  public AvroCoderCloudObjectTranslator(Class<T> coderClass, AvroCoderFactory<T> factory) {
    this.coderClass = coderClass;
    this.factory = factory;
  }

  @Override
  public CloudObject toCloudObject(AvroCoder target, SdkComponents sdkComponents) {
    CloudObject base = CloudObject.forClass(target.getClass());
    Structs.addString(base, SCHEMA_FIELD, target.getSchema().toString());
    Structs.addString(base, TYPE_FIELD, target.getType().getName());
    return base;
  }

  @Override
  public T fromCloudObject(CloudObject cloudObject) {
    Schema.Parser parser = new Schema.Parser();
    String className = Structs.getString(cloudObject, TYPE_FIELD);
    String schemaString = Structs.getString(cloudObject, SCHEMA_FIELD);
    try {
      Class<?> type = Class.forName(className);
      Schema schema = parser.parse(schemaString);
      return factory.apply(type, schema);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public Class<T> getSupportedClass() {
    return coderClass;
  }

  @Override
  public String cloudObjectClassName() {
    return CloudObject.forClass(coderClass).getClassName();
  }
}
