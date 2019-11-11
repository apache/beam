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
package org.apache.beam.sdk.io.thrift;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.thrift.parser.model.BaseType;
import org.apache.beam.sdk.io.thrift.parser.model.Const;
import org.apache.beam.sdk.io.thrift.parser.model.ConstInteger;
import org.apache.beam.sdk.io.thrift.parser.model.Document;
import org.apache.beam.sdk.io.thrift.parser.model.IntegerEnum;
import org.apache.beam.sdk.io.thrift.parser.model.IntegerEnumField;
import org.apache.beam.sdk.io.thrift.parser.model.Service;
import org.apache.beam.sdk.io.thrift.parser.model.StringEnum;
import org.apache.beam.sdk.io.thrift.parser.model.Struct;
import org.apache.beam.sdk.io.thrift.parser.model.ThriftException;
import org.apache.beam.sdk.io.thrift.parser.model.ThriftField;
import org.apache.beam.sdk.io.thrift.parser.model.ThriftMethod;
import org.apache.beam.sdk.io.thrift.parser.model.TypeAnnotation;
import org.apache.beam.sdk.io.thrift.parser.model.Typedef;
import org.apache.beam.sdk.io.thrift.parser.model.Union;
import org.apache.beam.sdk.io.thrift.parser.model.VoidType;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Document} class. */
@RunWith(JUnit4.class)
public class DocumentTest {

  /** Tests {@link Document#addIncludes(String)}. */
  @Test
  public void testAddIncludes() {
    Document document = Document.emptyDocument();
    List<String> includesExpected = new ArrayList<>();
    includesExpected.add("simple_test.thrift");
    includesExpected.add("shared.thrift");
    document.addIncludes(includesExpected);

    List<String> includesActual = document.getHeader().getIncludes();

    Assert.assertEquals(includesExpected, includesActual);
  }

  /** Tests {@link Document#addCppIncludes(List)}. */
  @Test
  public void testAddCppIncludes() {
    Document document = Document.emptyDocument();
    List<String> cppIncludesExpected = new ArrayList<>();
    cppIncludesExpected.add("iostream");
    cppIncludesExpected.add("set");
    document.addCppIncludes(cppIncludesExpected);

    List<String> cppIncludesActual = document.getHeader().getCppIncludes();

    Assert.assertEquals(cppIncludesExpected, cppIncludesActual);
  }

  /** Tests {@link Document#removeDefinition(String)}. */
  @Test
  public void testRemoveDefinition() {
    Document document = Document.emptyDocument();
    List<TypeAnnotation> emptyAnnotations = new ArrayList<>();
    String constName = "STRINGCONSTANT";
    document.addConstString(constName, emptyAnnotations, "test_string");
    Assert.assertEquals(1, document.getDefinitions().size());

    document.removeDefinition(constName);
    Assert.assertEquals(0, document.getDefinitions().size());
  }

  /** Tests {@link Document#addConst(String, ThriftType, ConstValue)}. */
  @Test
  public void testAddConst() {
    Document document = Document.emptyDocument();
    List<TypeAnnotation> emptyAnnotations = new ArrayList<>();
    String constName = "INT32CONSTANT";
    document.addConst(
        constName, new BaseType(BaseType.Type.I32, emptyAnnotations), new ConstInteger(252));

    String constNameActual = document.getDefinitions().get(0).getName();

    Assert.assertEquals(constName, constNameActual);
    Assert.assertTrue(document.getDefinitions().get(0) instanceof Const);
  }

  /** Tests {@link Document#addConstDouble(String, List, double)}. */
  @Test
  public void testAddConstDouble() {
    Document document = Document.emptyDocument();
    List<TypeAnnotation> emptyAnnotations = new ArrayList<>();
    String constName = "DOUBLECONSTANT";
    document.addConstDouble(constName, emptyAnnotations, 24.62);

    String constNameActual = document.getDefinitions().get(0).getName();

    Assert.assertEquals(constName, constNameActual);
  }

  /** Tests {@link Document#addConstIdentifier(String, List, String)}. */
  @Test
  public void testAddConstIdentifier() {
    Document document = Document.emptyDocument();
    List<TypeAnnotation> emptyAnnotations = new ArrayList<>();
    String constName = "STRINGCONSTANT";
    document.addConstIdentifier(constName, emptyAnnotations, "RANDOM_STRING");

    String constNameActual = document.getDefinitions().get(0).getName();

    Assert.assertEquals(constName, constNameActual);
  }

  /** Tests {@link Document#addConstInteger(String, BaseType.Type, List, Long)}. */
  @Test
  public void testAddConstInteger() {
    Document document = Document.emptyDocument();
    List<TypeAnnotation> emptyAnnotations = new ArrayList<>();
    String constName = "INT64CONSTANT";
    document.addConstInteger(constName, BaseType.Type.I64, emptyAnnotations, 42L);

    String constNameActual = document.getDefinitions().get(0).getName();

    Assert.assertEquals(constName, constNameActual);
  }

  /** Tests {@link Document#addConstList(String, BaseType.Type, List, String, List)}. */
  @Test
  public void testAddConstList() {
    Document document = Document.emptyDocument();
    List<TypeAnnotation> emptyAnnotations = new ArrayList<>();
    String constName = "LISTCONSTANT";
    List<Object> values = new ArrayList<>();
    values.add(new ConstInteger(52));
    values.add(new ConstInteger(27));
    document.addConstList(constName, BaseType.Type.I32, emptyAnnotations, null, values);

    String constNameActual = document.getDefinitions().get(0).getName();

    Assert.assertEquals(constName, constNameActual);
  }

  /**
   * Tests {@link Document#addConstMap(String, BaseType.Type, BaseType.Type, String, List, Map)}.
   */
  @Test
  public void testAddConstMap() {
    Document document = Document.emptyDocument();
    List<TypeAnnotation> emptyAnnotations = new ArrayList<>();
    String constName = "MAPCONSTANT";
    Map<Object, Object> values = new HashMap<>();
    values.put("value1", 73L);
    values.put("value2", 24L);

    document.addConstMap(
        constName, BaseType.Type.STRING, BaseType.Type.I32, "map", emptyAnnotations, values);

    String constNameActual = document.getDefinitions().get(0).getName();

    Assert.assertEquals(constName, constNameActual);
  }

  /** Tests {@link Document#addConstString(String, List, String)}. */
  @Test
  public void testAddConstString() {
    Document document = Document.emptyDocument();
    List<TypeAnnotation> emptyAnnotations = new ArrayList<>();
    String constName = "STRINGCONSTANT";
    String constValue = "test_string";

    document.addConstString(constName, emptyAnnotations, constValue);

    String constNameActual = document.getDefinitions().get(0).getName();
    String constValueActual =
        ((Const) document.getDefinitions().get(0)).getValue().value().toString();

    Assert.assertEquals(constName, constNameActual);
    Assert.assertEquals(constValue, constValueActual);
  }

  /** Tests {@link Document#addTypedef(String, ThriftType, List)}. */
  @Test
  public void testAddTypeDef() {
    Document document = Document.emptyDocument();
    List<TypeAnnotation> emptyAnnotations = new ArrayList<>();
    String typeDefName = "int";

    document.addTypedef(
        typeDefName, new BaseType(BaseType.Type.I32, emptyAnnotations), emptyAnnotations);

    String typeDeftNameActual = document.getDefinitions().get(0).getName();
    BaseType.Type typeActual =
        ((BaseType) ((Typedef) document.getDefinitions().get(0)).getType()).getType();

    Assert.assertEquals(typeDefName, typeDeftNameActual);
    Assert.assertEquals(BaseType.Type.I32, typeActual);
  }

  /** Tests {@link Document#addEnum(String, List, List)}. */
  @Test
  public void testAddEnum() {
    Document document = Document.emptyDocument();
    List<TypeAnnotation> emptyAnnotations = new ArrayList<>();
    String enumName = "testEnum";

    List<IntegerEnumField> enumFields = new ArrayList<>();
    enumFields.add(new IntegerEnumField("value1", 1L, 1L, emptyAnnotations));
    enumFields.add(new IntegerEnumField("value2", 2L, 2L, emptyAnnotations));

    document.addEnum(enumName, enumFields, emptyAnnotations);

    String enumNameActual = document.getDefinitions().get(0).getName();

    Assert.assertEquals(enumName, enumNameActual);
    Assert.assertTrue(document.getDefinitions().get(0) instanceof IntegerEnum);
  }

  /** Tests {@link Document#addSenum(String, List)}. */
  @Test
  public void testAddSenum() {
    Document document = Document.emptyDocument();
    List<TypeAnnotation> emptyAnnotations = new ArrayList<>();
    String senumName = "testSenum";

    List<String> senumFields = new ArrayList<>();
    senumFields.add("value1");
    senumFields.add("value2");

    document.addSenum(senumName, senumFields);

    String senumNameActual = document.getDefinitions().get(0).getName();

    Assert.assertEquals(senumName, senumNameActual);
    Assert.assertTrue(document.getDefinitions().get(0) instanceof StringEnum);
  }

  /** Tests {@link Document#addStruct(String, List, List)}. */
  @Test
  public void testAddStruct() {
    Document document = Document.emptyDocument();
    List<TypeAnnotation> emptyAnnotations = new ArrayList<>();
    String structName = "testStruct";

    List<ThriftField> structFields = new ArrayList<>();
    structFields.add(
        new ThriftField(
            "field1",
            new BaseType(BaseType.Type.I32, emptyAnnotations),
            1L,
            ThriftField.Requiredness.NONE,
            null,
            emptyAnnotations));
    structFields.add(
        new ThriftField(
            "field2",
            new BaseType(BaseType.Type.I32, emptyAnnotations),
            2L,
            ThriftField.Requiredness.NONE,
            null,
            emptyAnnotations));

    document.addStruct(structName, structFields, emptyAnnotations);

    String structNameActual = document.getDefinitions().get(0).getName();

    Assert.assertEquals(structName, structNameActual);
    Assert.assertTrue(document.getDefinitions().get(0) instanceof Struct);
  }

  /** Tests {@link Document#addUnion(String, List, List)}. */
  @Test
  public void testAddUnion() {
    Document document = Document.emptyDocument();
    List<TypeAnnotation> emptyAnnotations = new ArrayList<>();
    String unionName = "testUnion";

    List<ThriftField> unionFields = new ArrayList<>();
    unionFields.add(
        new ThriftField(
            "field1",
            new BaseType(BaseType.Type.I32, emptyAnnotations),
            1L,
            ThriftField.Requiredness.NONE,
            null,
            emptyAnnotations));
    unionFields.add(
        new ThriftField(
            "field2",
            new BaseType(BaseType.Type.I32, emptyAnnotations),
            2L,
            ThriftField.Requiredness.NONE,
            null,
            emptyAnnotations));

    document.addUnion(unionName, unionFields, emptyAnnotations);

    String unionNameActual = document.getDefinitions().get(0).getName();

    Assert.assertEquals(unionName, unionNameActual);
    Assert.assertTrue(document.getDefinitions().get(0) instanceof Union);
  }

  /** Tests {@link Document#addThriftException(String, List, List)}. */
  @Test
  public void testAddThriftException() {
    Document document = Document.emptyDocument();
    List<TypeAnnotation> emptyAnnotations = new ArrayList<>();
    String exceptionName = "testException";

    List<ThriftField> exceptionFields = new ArrayList<>();
    exceptionFields.add(
        new ThriftField(
            "whatOp",
            new BaseType(BaseType.Type.I32, emptyAnnotations),
            1L,
            ThriftField.Requiredness.NONE,
            null,
            emptyAnnotations));
    exceptionFields.add(
        new ThriftField(
            "why",
            new BaseType(BaseType.Type.I32, emptyAnnotations),
            2L,
            ThriftField.Requiredness.NONE,
            null,
            emptyAnnotations));

    document.addThriftException(exceptionName, exceptionFields, emptyAnnotations);

    String exceptionNameActual = document.getDefinitions().get(0).getName();

    Assert.assertEquals(exceptionName, exceptionNameActual);
    Assert.assertTrue(document.getDefinitions().get(0) instanceof ThriftException);
  }

  /** Tests {@link Document#addService(String, String, List, List)}. */
  @Test
  public void testAddService() {
    Document document = Document.emptyDocument();
    List<TypeAnnotation> emptyAnnotations = new ArrayList<>();
    List<ThriftField> emptyArgs = new ArrayList<>();
    List<ThriftField> emptyThrows = new ArrayList<>();
    String servicenName = "testService";

    List<ThriftMethod> serviceMethods = new ArrayList<>();
    serviceMethods.add(
        new ThriftMethod(
            "method1", new VoidType(), emptyArgs, false, emptyThrows, emptyAnnotations));
    serviceMethods.add(
        new ThriftMethod(
            "method2", new VoidType(), emptyArgs, false, emptyThrows, emptyAnnotations));

    document.addService(servicenName, null, serviceMethods, emptyAnnotations);

    String serviceNameActual = document.getDefinitions().get(0).getName();

    Assert.assertEquals(servicenName, serviceNameActual);
    Assert.assertTrue(document.getDefinitions().get(0) instanceof Service);
  }
}
