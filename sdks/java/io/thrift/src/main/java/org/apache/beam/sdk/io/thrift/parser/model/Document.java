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
package org.apache.beam.sdk.io.thrift.parser.model;

import static java.util.Collections.emptyList;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.reflect.ReflectData;
import org.apache.beam.sdk.io.thrift.parser.visitor.DocumentVisitor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

/**
 * The {@link Document} class holds the elements of a Thrift file.
 *
 * <p>A {@link Document} is made up of:
 *
 * <ul>
 *   <li>{@link Header} - Contains: includes, cppIncludes, namespaces, and defaultNamespace.
 *   <li>{@link Document#definitions} - Contains list of Thrift {@link Definition}.
 * </ul>
 */
public class Document implements Serializable {
  private Header header;
  private List<Definition> definitions;

  public Document(Header header, List<Definition> definitions) {
    this.header = checkNotNull(header, "header");
    this.definitions = ImmutableList.copyOf(checkNotNull(definitions, "definitions"));
  }

  /** Returns an empty {@link Document}. */
  public static Document emptyDocument() {
    List<String> includes = emptyList();
    List<String> cppIncludes = emptyList();
    String defaultNamespace = null;
    Map<String, String> namespaces = Collections.emptyMap();
    Header header = new Header(includes, cppIncludes, defaultNamespace, namespaces);
    List<Definition> definitions = emptyList();
    return new Document(header, definitions);
  }

  public Document getDocument() {
    return this;
  }

  public Header getHeader() {
    return this.header;
  }

  public void setHeader(Header header) {
    this.header = header;
  }

  public List<Definition> getDefinitions() {
    return definitions;
  }

  public void setDefinitions(List<Definition> definitions) {
    this.definitions = definitions;
  }

  public void visit(final DocumentVisitor visitor) throws IOException {
    Preconditions.checkNotNull(visitor, "the visitor must not be null!");

    for (Definition definition : definitions) {
      if (visitor.accept(definition)) {
        definition.visit(visitor);
      }
    }
  }

  /** Gets Avro {@link Schema} for the object. */
  public Schema getSchema() {
    return ReflectData.get().getSchema(Document.class);
  }

  /** Gets {@link Document} as a {@link GenericRecord}. */
  public GenericRecord getAsGenericRecord() {
    GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(this.getSchema());
    genericRecordBuilder.set("header", this.getHeader()).set("definitions", this.getDefinitions());

    return genericRecordBuilder.build();
  }

  /** Adds list of includes to {@link Document#header}. */
  public void addIncludes(List<String> includes) {
    checkNotNull(includes, "includes");
    List<String> currentIncludes = new ArrayList<>(this.getHeader().getIncludes());
    currentIncludes.addAll(includes);
    this.header.setIncludes(currentIncludes);
  }

  /** Adds single string include to {@link Document#header}. */
  public void addIncludes(String includes) {
    addIncludes(Collections.singletonList(includes));
  }

  /** Adds list of cpp includes to {@link Document#header}. */
  public void addCppIncludes(List<String> cppIncludes) {
    checkNotNull(cppIncludes, "includes");
    List<String> currentCppIncludes = new ArrayList<>(this.getHeader().getCppIncludes());
    currentCppIncludes.addAll(cppIncludes);
    this.header.setCppIncludes(currentCppIncludes);
  }

  /** Adds single cpp includes to {@link Document#header}. */
  public void addCppIncludes(String cppIncludes) {
    addCppIncludes(Collections.singletonList(cppIncludes));
  }

  /** Adds a {@link Definition} to {@link Document#definitions}. */
  private void addDefinition(Definition definition) {
    List<Definition> definitions = this.getDefinitions();
    this.setDefinitions(
        new ImmutableList.Builder<Definition>().addAll(definitions).add(definition).build());
  }

  /** Removes a {@link Definition} from {@link Document#definitions}. */
  public void removeDefinition(String name) {
    List<Definition> definitions = new ArrayList<>(this.getDefinitions());
    definitions.removeIf(definition -> definition.getName().equals(name));
    this.setDefinitions(new ImmutableList.Builder<Definition>().addAll(definitions).build());
  }

  /**
   * Adds {@link Const} to list of Thrift definitions.
   *
   * @param name name of the constant
   * @param thriftType {@link ThriftType} for the constant.
   * @param constValue {@link ConstValue} of the constant.
   */
  public void addConst(String name, ThriftType thriftType, ConstValue constValue) {
    this.addDefinition(new Const(name, thriftType, constValue));
  }

  /**
   * Adds a {@link ConstDouble}.
   *
   * @param name name of the constant.
   * @param annotations list of {@link TypeAnnotation} for the constant.
   * @param value value of the constant.
   */
  public void addConstDouble(String name, List<TypeAnnotation> annotations, double value) {
    addConst(name, new BaseType(BaseType.Type.DOUBLE, annotations), new ConstDouble(value));
  }

  /**
   * Adds a {@link ConstIdentifier}.
   *
   * @param name name of the constant.
   * @param annotations list of {@link TypeAnnotation} for the constant.
   * @param value value of the constant.
   */
  public void addConstIdentifier(String name, List<TypeAnnotation> annotations, String value) {
    addConst(name, new BaseType(BaseType.Type.STRING, annotations), new ConstString(value));
  }

  /**
   * Adds a {@link ConstInteger}.
   *
   * @param name name of the constant.
   * @param type {@link ThriftType} of the constant.
   * @param annotations list of {@link TypeAnnotation} for the constant.
   * @param value value of the constant.
   */
  public void addConstInteger(
      String name, BaseType.Type type, List<TypeAnnotation> annotations, Long value) {
    addConst(name, new BaseType(type, annotations), new ConstInteger(value));
  }

  /**
   * Adds a {@link ConstList}.
   *
   * @param name name of the constant.
   * @param type {@link ThriftType} of the list.
   * @param annotations list of {@link TypeAnnotation} for the constant.
   * @param cppType Cpp type for the list.
   * @param values List values.
   */
  public void addConstList(
      String name,
      BaseType.Type type,
      List<TypeAnnotation> annotations,
      String cppType,
      List<Object> values) {

    ThriftType thriftType = new BaseType(type, annotations);
    ListType listType = new ListType(thriftType, cppType, annotations);
    List<ConstValue> constValuesList = getConstValueList(values);

    addConst(name, listType, new ConstList(constValuesList));
  }

  /**
   * Adds a {@link ConstMap}.
   *
   * @param name string name of the map.
   * @param keyType {@link ThriftType} of the key
   * @param valueType {@link ThriftType} of the value
   * @param cppType Cpp type of the map
   * @param annotations list of {@link TypeAnnotation} for the map.
   * @param values Map of values
   */
  public void addConstMap(
      String name,
      BaseType.Type keyType,
      BaseType.Type valueType,
      String cppType,
      List<TypeAnnotation> annotations,
      Map<Object, Object> values) {

    ThriftType keyTType = new BaseType(keyType, annotations);
    ThriftType valueTType = new BaseType(valueType, annotations);
    MapType mapType = new MapType(keyTType, valueTType, cppType, annotations);
    Map<ConstValue, ConstValue> constValueMap = getConstValueMap(keyType, valueType, values);

    addConst(name, mapType, new ConstMap(constValueMap));
  }

  /**
   * Adds a {@link ConstString}.
   *
   * @param name string name of the constant
   * @param annotations list of {@link TypeAnnotation} for the constant.
   * @param value String value
   */
  public void addConstString(String name, List<TypeAnnotation> annotations, String value) {
    addConst(name, new BaseType(BaseType.Type.STRING, annotations), new ConstString(value));
  }

  /**
   * Adds a {@link Typedef}.
   *
   * @param name name of the Typedef.
   * @param thriftType {@link ThriftType} of the Typedef.
   */
  public void addTypedef(String name, ThriftType thriftType, List<TypeAnnotation> annotations) {
    this.addDefinition(new Typedef(name, thriftType, annotations));
  }

  /**
   * Adds an {@link IntegerEnum}.
   *
   * @param name string name of the {@link IntegerEnum}
   * @param fields list of {@link IntegerEnumField}
   */
  public void addEnum(
      String name, List<IntegerEnumField> fields, List<TypeAnnotation> annotations) {
    this.addDefinition(new IntegerEnum(name, fields, annotations));
  }

  /**
   * Adds an {@link StringEnum}.
   *
   * @param name string name of the {@link StringEnum}.
   * @param fields List of strings making up the fields of the enum.
   */
  public void addSenum(String name, List<String> fields) {
    this.addDefinition(new StringEnum(name, fields));
  }

  /**
   * Adds an {@link Struct}.
   *
   * @param name string name of the struct
   * @param fields list of {@link ThriftField}
   * @param annotations list of {@link TypeAnnotation} for the struct
   */
  public void addStruct(String name, List<ThriftField> fields, List<TypeAnnotation> annotations) {
    this.addDefinition(new Struct(name, fields, annotations));
  }

  /**
   * Adds an {@link Union}.
   *
   * @param name string name of the union
   * @param fields list of {@link ThriftField}
   * @param annotations list of {@link TypeAnnotation} for the union
   */
  public void addUnion(String name, List<ThriftField> fields, List<TypeAnnotation> annotations) {
    this.addDefinition(new Union(name, fields, annotations));
  }

  /**
   * Adds a {@link ThriftException}.
   *
   * @param name string name of the exception
   * @param fields list of {@link ThriftField}
   * @param annotations list of {@link TypeAnnotation} for the exception
   */
  public void addThriftException(
      String name, List<ThriftField> fields, List<TypeAnnotation> annotations) {
    this.addDefinition(new ThriftException(name, fields, annotations));
  }

  /**
   * Adds a {@link Service}.
   *
   * @param name string name of the service
   * @param parent string parent of the service
   * @param methods list of {@link ThriftMethod}
   * @param annotations list of {@link TypeAnnotation} for the service
   */
  public void addService(
      String name, String parent, List<ThriftMethod> methods, List<TypeAnnotation> annotations) {
    this.addDefinition(new Service(name, parent, methods, annotations));
  }

  /**
   * Converts list of objects to {@link ConstValue}.
   *
   * @param objectList list of objects to be converted.
   * @return list of objects converted to {@link ConstValue}.
   */
  private static List<ConstValue> getConstValueList(List<Object> objectList) {
    List<ConstValue> constValues = new ArrayList<>();
    objectList.forEach(
        obj -> {
          constValues.add((ConstValue) obj);
        });
    return constValues;
  }

  /**
   * Converts input map to map with key and value as {@link ConstValue}.
   *
   * @param keyType Thrift base type for the key.
   * @param valueType Thrift base type for the value.
   * @param objectMap map value to be converted.
   * @return map with key and value wrapped with {@link ConstValue}.
   */
  private static Map<ConstValue, ConstValue> getConstValueMap(
      BaseType.Type keyType, BaseType.Type valueType, Map<Object, Object> objectMap) {
    Map<ConstValue, ConstValue> constValueMap = new HashMap<>();
    objectMap.forEach(
        (k, v) -> {
          constValueMap.put(getConstValue(keyType, k), getConstValue(valueType, v));
        });
    return constValueMap;
  }

  /**
   * Gets value as a {@link ConstValue}.
   *
   * @param baseType Thrift base type for the value.
   * @param value the value object.
   * @return ConstValue of passed value.
   */
  private static ConstValue getConstValue(BaseType.Type baseType, Object value) {
    switch (baseType) {
      case STRING:
        return new ConstString((String) value);
      case BYTE:
      case I64:
      case I32:
      case I16:
        return new ConstInteger((long) value);
      case DOUBLE:
        return new ConstDouble((double) value);
      case BINARY:
        return new ConstBinary((byte[]) value);
      case BOOL:
        return new ConstBool((boolean) value);
      default:
        throw new RuntimeException("Invalid type passed: " + baseType);
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("header", header)
        .add("definitions", definitions)
        .toString();
  }

  @Override
  public boolean equals(Object o) {

    if (o == this) {
      return true;
    }

    if (!(o instanceof Document)) {
      return false;
    }

    Document c = (Document) o;

    return Objects.equals(header, c.header) && Objects.equals(definitions, c.definitions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(header, definitions);
  }
}
