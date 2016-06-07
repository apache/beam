/**
 * Copyright (c) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not  use this file except  in compliance with the License. You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.contrib.firebase.utils;

import static com.google.cloud.dataflow.sdk.util.Structs.addString;
import com.google.cloud.dataflow.contrib.firebase.contrib.JacksonCoder;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;



/**
 * @see JacksonCoder
 * Subclassed to allow parameterized type to behave property
 * @param <T> Type of {@link Record} encoded by {@link FirebaseCheckpoint}
 */
public class FirebaseCheckpointCoder<T> extends JacksonCoder<FirebaseCheckpoint<T>> {
  private final Class<T> subType;

  @SuppressWarnings("unchecked")
  public FirebaseCheckpointCoder(TypeDescriptor<FirebaseCheckpoint<T>> type, Class<T> subType) {
    this((Class<FirebaseCheckpoint<T>>) type.getRawType(), subType);
  }

  public FirebaseCheckpointCoder(Class<FirebaseCheckpoint<T>> type, Class<T> subType) {
    super(type);
    this.subType = subType;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @JsonCreator
  public static FirebaseCheckpointCoder<?> of(
      @JsonProperty("type") String classType,
      @JsonProperty("subType") String subClassType) throws ClassNotFoundException {
    return new FirebaseCheckpointCoder(Class.forName(classType), Class.forName(subClassType));
  }

  public static <K> FirebaseCheckpointCoder<K> of(
      TypeDescriptor<FirebaseCheckpoint<K>> type,
      Class<K> subType)  {
    return new FirebaseCheckpointCoder<K>(type, subType);
  }

  @Override
  protected Object writeReplace() {
    // When serialized by Java, instances of FirebaseCheckpointCoder should be replaced by
    // a SerializedAvroCoderProxy.
    return new FirebaseJacksonCoderProxy<>(type, subType);
  }

  @Override
  public CloudObject asCloudObject() {
    CloudObject result = super.asCloudObject();
    addString(result, "subType", subType.getName());
    return result;
  }

  public Class<T> getSubType(){
    return this.subType;
  }


  /**
   * Serialization proxy to allow fields to be final.
   * @param <T> Type of {@link Record} encoded by {@link FirebaseCheckpoint}
   */
  protected static class FirebaseJacksonCoderProxy<T>
    extends SerializedJacksonCoderProxy<FirebaseCheckpoint<T>>{

    private final Class<T> subType;

    /**
     * @param type
     */
    public FirebaseJacksonCoderProxy(Class<FirebaseCheckpoint<T>> type, Class<T> subType) {
      super(type);
      this.subType = subType;
    }

    private Object readResolve() {
      // When deserialized, instances of this object should be replaced by
      // constructing a FirebaseJacksonCoder.
      return new FirebaseCheckpointCoder<T>(this.type, this.subType);
    }

  }

}
