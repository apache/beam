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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.util.construction.SdkComponents;

/** A {@link CloudObjectTranslator} for {@link SerializableCoder}. */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
class SerializableCoderCloudObjectTranslator implements CloudObjectTranslator<SerializableCoder> {
  private static final String TYPE_FIELD = "type";

  @Override
  public CloudObject toCloudObject(SerializableCoder target, SdkComponents sdkComponents) {
    CloudObject base = CloudObject.forClass(SerializableCoder.class);
    Structs.addString(base, TYPE_FIELD, target.getRecordType().getName());
    return base;
  }

  @Override
  public SerializableCoder<?> fromCloudObject(CloudObject cloudObject) {
    String className = Structs.getString(cloudObject, TYPE_FIELD);
    try {
      Class<? extends Serializable> targetClass =
          (Class<? extends Serializable>)
              Class.forName(className, false, Thread.currentThread().getContextClassLoader());
      checkArgument(
          Serializable.class.isAssignableFrom(targetClass),
          "Target class %s does not extend %s",
          targetClass.getName(),
          Serializable.class.getSimpleName());
      return SerializableCoder.of(targetClass);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public Class<SerializableCoder> getSupportedClass() {
    return SerializableCoder.class;
  }

  @Override
  public String cloudObjectClassName() {
    return CloudObject.forClass(SerializableCoder.class).getClassName();
  }
}
