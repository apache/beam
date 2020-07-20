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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.json.GenericJson;
import com.google.api.client.util.Key;
import java.util.Map;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A representation of an arbitrary Java object to be instantiated by Dataflow workers.
 *
 * <p>Typically, an object to be written by the SDK to the Dataflow service will implement a method
 * (typically called {@code asCloudObject()}) that returns a {@code CloudObject} to represent the
 * object in the protocol. Once the {@code CloudObject} is constructed, the method should explicitly
 * add additional properties to be presented during deserialization, representing child objects by
 * building additional {@code CloudObject}s.
 */
public final class CloudObject extends GenericJson implements Cloneable {
  /**
   * Constructs a {@code CloudObject} by copying the supplied serialized object spec, which must
   * represent an SDK object serialized for transport via the Dataflow API.
   *
   * <p>The most common use of this method is during deserialization on the worker, where it's used
   * as a binding type during instance construction.
   *
   * @param spec supplies the serialized form of the object as a nested map
   * @throws RuntimeException if the supplied map does not represent an SDK object
   */
  public static CloudObject fromSpec(Map<String, Object> spec) {
    CloudObject result = new CloudObject();
    result.putAll(spec);
    if (result.className == null) {
      throw new RuntimeException(
          "Unable to create an SDK object from "
              + spec
              + ": Object class not specified (missing \""
              + PropertyNames.OBJECT_TYPE_NAME
              + "\" field)");
    }
    return result;
  }

  /**
   * Constructs a {@code CloudObject} to be used for serializing an instance of the supplied class
   * for transport via the Dataflow API. The instance parameters to be serialized must be supplied
   * explicitly after the {@code CloudObject} is created, by using {@link CloudObject#put}.
   *
   * @param cls the class to use when deserializing the object on the worker
   */
  public static CloudObject forClass(Class<?> cls) {
    CloudObject result = new CloudObject();
    result.className = checkNotNull(cls).getName();
    return result;
  }

  /**
   * Constructs a {@code CloudObject} to be used for serializing data to be deserialized using the
   * supplied class name the supplied class name for transport via the Dataflow API. The instance
   * parameters to be serialized must be supplied explicitly after the {@code CloudObject} is
   * created, by using {@link CloudObject#put}.
   *
   * @param className the class to use when deserializing the object on the worker
   */
  public static CloudObject forClassName(String className) {
    CloudObject result = new CloudObject();
    result.className = checkNotNull(className);
    return result;
  }

  /**
   * Constructs a {@code CloudObject} representing the given value.
   *
   * @param value the scalar value to represent.
   */
  public static CloudObject forString(String value) {
    CloudObject result = forClassName(CloudKnownType.TEXT.getUri());
    result.put(PropertyNames.SCALAR_FIELD_NAME, value);
    return result;
  }

  /**
   * Constructs a {@code CloudObject} representing the given value.
   *
   * @param value the scalar value to represent.
   */
  public static CloudObject forBoolean(Boolean value) {
    CloudObject result = forClassName(CloudKnownType.BOOLEAN.getUri());
    result.put(PropertyNames.SCALAR_FIELD_NAME, value);
    return result;
  }

  /**
   * Constructs a {@code CloudObject} representing the given value.
   *
   * @param value the scalar value to represent.
   */
  public static CloudObject forInteger(Long value) {
    CloudObject result = forClassName(CloudKnownType.INTEGER.getUri());
    result.put(PropertyNames.SCALAR_FIELD_NAME, value);
    return result;
  }

  /**
   * Constructs a {@code CloudObject} representing the given value.
   *
   * @param value the scalar value to represent.
   */
  public static CloudObject forInteger(Integer value) {
    CloudObject result = forClassName(CloudKnownType.INTEGER.getUri());
    result.put(PropertyNames.SCALAR_FIELD_NAME, value);
    return result;
  }

  /**
   * Constructs a {@code CloudObject} representing the given value.
   *
   * @param value the scalar value to represent.
   */
  public static CloudObject forFloat(Float value) {
    CloudObject result = forClassName(CloudKnownType.FLOAT.getUri());
    result.put(PropertyNames.SCALAR_FIELD_NAME, value);
    return result;
  }

  /**
   * Constructs a {@code CloudObject} representing the given value.
   *
   * @param value the scalar value to represent.
   */
  public static CloudObject forFloat(Double value) {
    CloudObject result = forClassName(CloudKnownType.FLOAT.getUri());
    result.put(PropertyNames.SCALAR_FIELD_NAME, value);
    return result;
  }

  /**
   * Constructs a {@code CloudObject} representing the given value of a well-known cloud object
   * type.
   *
   * @param value the scalar value to represent.
   * @throws RuntimeException if the value does not have a {@link CloudKnownType} mapping
   */
  public static CloudObject forKnownType(Object value) {
    @Nullable CloudKnownType ty = CloudKnownType.forClass(value.getClass());
    if (ty == null) {
      throw new RuntimeException("Unable to represent value via the Dataflow API: " + value);
    }
    CloudObject result = forClassName(ty.getUri());
    result.put(PropertyNames.SCALAR_FIELD_NAME, value);
    return result;
  }

  @Key(PropertyNames.OBJECT_TYPE_NAME)
  private String className;

  private CloudObject() {}

  /** Gets the name of the Java class that this CloudObject represents. */
  public String getClassName() {
    return className;
  }

  @Override
  public CloudObject clone() {
    return (CloudObject) super.clone();
  }

  @Override
  public boolean equals(@Nullable Object otherObject) {
    if (!(otherObject instanceof CloudObject)) {
      return false;
    }
    CloudObject other = (CloudObject) otherObject;
    return Objects.equals(className, other.className) && super.equals(otherObject);
  }

  @Override
  public int hashCode() {
    return Objects.hash(className, super.hashCode());
  }
}
