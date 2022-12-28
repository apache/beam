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
package org.apache.beam.sdk.io.fileschematransform;

import java.io.Serializable;
import java.io.Writer;
import java.util.HashMap;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.Xml;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

/**
 * Wraps a {@link Row} for compatible use with {@link javax.xml.bind.JAXBContext}. {@link
 * XmlRowAdapter} allows {@link Xml} to convert {@link Row} to XML strings with no knowledge of the
 * original Java class. {@link javax.xml.bind.Marshaller} requires Java classes to be annotated with
 * {@link XmlRootElement}, preventing the {@link javax.xml.bind.Marshaller#marshal(Object, Writer)}
 * of {@link Row}s directly. {@link XmlRowAdapter} exposes the String key and Object value pairs of
 * the {@link Row} to the {@link javax.xml.bind.Marshaller}.
 */
@XmlRootElement(name = "row")
@XmlAccessorType(XmlAccessType.PROPERTY)
class XmlRowAdapter implements Serializable {

  private final HashMap<String, XmlRowValue> record = new HashMap<>();

  /**
   * Wrap a {@link Row} to prepare {@link XmlRowAdapter}'s use with {@link
   * javax.xml.bind.Marshaller}. {@link XmlRowAdapter} stores a {@link HashMap} that this method
   * fills from {@link Row}'s String key and Object value pairs. This copying of data to the {@link
   * HashMap} allows {@link javax.xml.bind.Marshaller} to populate the XML elements from {@link
   * #getData()}.
   */
  void wrapRow(Row row) {
    Schema schema = row.getSchema();
    for (String key : schema.getFieldNames()) {
      XmlRowValue value = new XmlRowValue();
      value.setValue(key, row);
      record.put(key, value);
    }
  }

  // private Object getValue(String key, Field field, Row row) {
  //   switch (field.getType().getTypeName()) {
  //     case ROW:
  //       return getRowValue(key, row);
  //     case BOOLEAN:
  //     case BYTE:
  //     case FLOAT:
  //     case INT16:
  //     case INT32:
  //     case INT64:
  //     case STRING:
  //     case DOUBLE:
  //     case DECIMAL:
  //       return row.getValue(key);
  //     case MAP:
  //       return getMapValue(key, row);
  //     case ARRAY:
  //       return getArrayValue(key, row);
  //     default:
  //       throw new IllegalStateException(
  //           String.format("%s of %s is not supported", key,
  // field.getType().getTypeName().name()));
  //   }
  // }

  // private Object getArrayValue(String key, Row parent) {
  //   Optional<Collection<?>> array = Optional.ofNullable(parent.getArray(key));
  //   checkState(array.isPresent());
  //   return array.get();
  // }

  // private Object getMapValue(String key, Row parent) {
  //   Optional<Map<?, ?>> map = Optional.ofNullable(parent.getMap(key));
  //   checkState(map.isPresent());
  //   return map.get();
  // }

  // private Object getRowValue(String key, Row parent) {
  //   Optional<Row> child = Optional.ofNullable(parent.getRow(key));
  //   checkState(child.isPresent());
  //   XmlRowAdapter wrapper = new XmlRowAdapter();
  //   wrapper.wrapRow(child.get());
  //   return wrapper.record;
  // }

  /**
   * Exposes the copied {@link Row} data to the {@link javax.xml.bind.Marshaller} via the {@link
   * XmlElement} annotation.
   */
  @XmlElement
  HashMap<String, XmlRowValue> getData() {
    return record;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    XmlRowAdapter that = (XmlRowAdapter) o;
    return record.equals(that.record);
  }

  @Override
  public int hashCode() {
    return Objects.hash(record);
  }

  @Override
  public String toString() {
    return "XmlRowAdapter{" + "record=" + record + '}';
  }
}
