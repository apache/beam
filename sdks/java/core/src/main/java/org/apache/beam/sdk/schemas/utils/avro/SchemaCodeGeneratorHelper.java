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

package org.apache.beam.sdk.schemas.utils.avro;

import com.sun.codemodel.JClass;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JInvocation;
import org.apache.beam.sdk.schemas.logicaltypes.CharType;
import org.apache.beam.sdk.schemas.utils.avro.exceptions.SchemaAssistantException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.logicaltypes.FixedBytes;
import org.apache.beam.sdk.values.Row;

public class SchemaCodeGeneratorHelper {

  private static final String SEP = "_";
  private final JCodeModel codeModel;
  private Set<Class<? extends Exception>> exceptionsFromStringable;
  // This is used to collect all the fully qualified classes being used by the generated class. The
  // purpose of this bookkeeping is that in some framework, those classes couldn't be found during
  // compilation time, so we need to manually find the libraries, which define those classes, and
  // put them in the compile classpath.
  private final Set<String> fullyQualifiedClassNameSet = new HashSet<>();

  /**
   * Common functions is used by Code Generator.
   *
   * @param codeModel {@link JCodeModel}
   */
  public SchemaCodeGeneratorHelper(JCodeModel codeModel) {
    this.codeModel = codeModel;
    this.exceptionsFromStringable = new HashSet<>();
  }

  protected Set<String> getUsedFullyQualifiedClassNameSet() {
    return fullyQualifiedClassNameSet;
  }

  public void resetExceptionsFromStringable() {
    exceptionsFromStringable = new HashSet<>();
  }

  public Set<Class<? extends Exception>> getExceptionsFromStringable() {
    return exceptionsFromStringable;
  }

  public void setExceptionsFromStringable(
      Set<Class<? extends Exception>> exceptionsFromStringable) {
    this.exceptionsFromStringable = exceptionsFromStringable;
  }

  /**
   * Returns Map's Key's class reference.
   *
   * @param schema Map's Avro schema.
   * @return
   */
  public JClass keyClassFromMapSchema(Schema schema) {
    if (!Schema.Type.MAP.equals(schema.getType())) {
      throw new SchemaAssistantException(
          "Map schema was expected, instead got:" + schema.getType().getName());
    }
    return codeModel.ref(String.class);
  }

  private JClass valueClassFromMapSchema(Schema schema) {
    if (!Schema.Type.MAP.equals(schema.getType())) {
      throw new SchemaAssistantException(
          "Map schema was expected, instead got:" + schema.getType().getName());
    }

    return classFromSchema(schema.getValueType());
  }

  private JClass elementClassFromArraySchema(Schema schema) {
    if (!Schema.Type.ARRAY.equals(schema.getType())) {
      throw new SchemaAssistantException(
          "Array schema was expected, instead got:" + schema.getType().getName());
    }

    return classFromSchema(schema.getElementType());
  }

  private JClass classFromUnionSchema(final Schema schema) {
    if (!Schema.Type.UNION.equals(schema.getType())) {
      throw new SchemaAssistantException(
          "Union schema was expected, instead got:" + schema.getType().getName());
    }

    if (schema.getTypes().size() == 1) {
      return classFromSchema(schema.getTypes().get(0));
    }

    if (schema.getTypes().size() == 2) {
      if (Schema.Type.NULL.equals(schema.getTypes().get(0).getType())) {
        return classFromSchema(schema.getTypes().get(1));
      } else if (Schema.Type.NULL.equals(schema.getTypes().get(1).getType())) {
        return classFromSchema(schema.getTypes().get(0));
      }
    }

    return codeModel.ref(Object.class);
  }

  public JClass classFromSchema(Schema schema) {
    return classFromSchema(schema, true, false);
  }

  public JClass classFromSchema(Schema schema, boolean abstractType) {
    return classFromSchema(schema, abstractType, false);
  }

  /* Note that settings abstractType and rawType are not passed to subcalls */

  /**
   * Helper for deciding output format while generating java code based on a schema definition. Note
   * that settings abstractType and rawType are not passed to subcalls
   *
   * @param schema       Avro Schema
   * @param abstractType True returns {@link List}, False returns {@link ArrayList} for an array.
   * @param rawType      False returns {@code List<String>}. True returns List.
   * @return
   */
  public JClass classFromSchema(Schema schema, boolean abstractType, boolean rawType) {
    JClass outputClass;

    switch (schema.getType()) {
      case RECORD:
        outputClass = codeModel.ref(Row.class);
        break;
      case ARRAY:
        if (abstractType) {
          outputClass = codeModel.ref(List.class);
        } else {
          outputClass = codeModel.ref(ArrayList.class);
        }
        if (!rawType) {
          outputClass = outputClass.narrow(elementClassFromArraySchema(schema));
        }
        break;
      case MAP:
        if (!abstractType) {
          outputClass = codeModel.ref(HashMap.class);
        } else {
          outputClass = codeModel.ref(Map.class);
        }
        if (!rawType) {
          outputClass = outputClass
              .narrow(keyClassFromMapSchema(schema), valueClassFromMapSchema(schema));
        }
        break;
      case UNION:
        outputClass = classFromUnionSchema(schema);
        break;
      case ENUM:
        outputClass = codeModel.ref(String.class);
        break;
      case FIXED:
        if (abstractType) {
          outputClass = codeModel.ref(Object.class);
        } else {
          outputClass = codeModel.BYTE.array();
        }
        break;
      case BOOLEAN:
        outputClass = codeModel.ref(Boolean.class);
        break;
      case DOUBLE:
        outputClass = codeModel.ref(Double.class);
        break;
      case FLOAT:
        outputClass = codeModel.ref(Float.class);
        break;
      case INT:
        outputClass = codeModel.ref(Integer.class);
        break;
      case LONG:
        outputClass = codeModel.ref(Long.class);
        break;
      case STRING:
        outputClass = codeModel.ref(String.class);
        break;
      case BYTES:
        outputClass = codeModel.ref(ByteBuffer.class);
        break;
      default:
        throw new SchemaAssistantException(
            "Incorrect request for " + schema.getType().getName() + " class!");
    }

    // Exclude the narrowed type.
    // Essentially, for type: java.util.ArrayList<java.util.Map<org.apache.avro.util.Utf8>,
    // {@link JClass#erasure()} will return java.util.ArrayList, and that is the class,
    // which can be located by {@link Class#forName(String)}.
    fullyQualifiedClassNameSet.add(outputClass.erasure().fullName());

    return outputClass;
  }


  /* Note that settings abstractType and rawType are not passed to subcalls */

  /**
   * Helper for deciding output format while generating java code based on a field definition.
   *
   * @param fieldType       Field in a beam schema
   * @return
   */
  public JClass classFromFieldType(FieldType fieldType) {
    JClass outputClass;
    org.apache.beam.sdk.schemas.Schema.TypeName typeName = fieldType.getTypeName();
    switch (typeName) {
      case ROW:
        outputClass = codeModel.ref(Row.class);
        break;
      case ARRAY:
        outputClass = codeModel.ref(List.class);
        outputClass = outputClass.narrow(classFromFieldType(fieldType.getCollectionElementType()));
        break;
      case BOOLEAN:
        outputClass = codeModel.ref(Boolean.class);
        break;
      case DOUBLE:
        outputClass = codeModel.ref(Double.class);
        break;
      case FLOAT:
        outputClass = codeModel.ref(Float.class);
        break;
      case BYTE:
      case INT16:
      case INT32:
        outputClass = codeModel.ref(Integer.class);
        break;
      case INT64:
        outputClass = codeModel.ref(Long.class);
        break;
      case STRING:
        outputClass = codeModel.ref(String.class);
        break;
      case BYTES:
        outputClass = codeModel.ref(byte[].class);
        break;
      case LOGICAL_TYPE:
        switch (fieldType.getLogicalType().getIdentifier()) {
          case FixedBytes.IDENTIFIER:
            return classFromFieldType(FieldType.BYTES);
          case EnumerationType.IDENTIFIER:
          case CharType.IDENTIFIER:
            return classFromFieldType(FieldType.STRING);
          default:
            throw new SchemaAssistantException(
                    "Unhandled logical type " + fieldType.getLogicalType().getIdentifier());
        }
      default:
        throw new SchemaAssistantException(
                "Incorrect request for " + typeName + " class!");
    }

    // Exclude the narrowed type.
    // Essentially, for type: java.util.ArrayList<java.util.Map<org.apache.avro.util.Utf8>,
    // {@link JClass#erasure()} will return java.util.ArrayList, and that is the class,
    // which can be located by {@link Class#forName(String)}.
    fullyQualifiedClassNameSet.add(outputClass.erasure().fullName());

    return outputClass;
  }

  public JExpression getEnumValueByIndex(Schema enumSchema, JExpression indexExpr,
      JInvocation getSchemaExpr) {
    return getSchemaExpr.invoke("getEnumSymbols").invoke("get").arg(indexExpr);
  }

  public JExpression getFixedValue(Schema schema, JExpression fixedBytesExpr) {
    return JExpr._new(codeModel.ref(schema.getFullName())).arg(fixedBytesExpr);
  }

  /**
   * Complex type needs to handle other types inside itself.
   *
   * @param schema Complex type's avro schema.
   * @return
   */
  public static boolean isComplexType(Schema schema) {
    switch (schema.getType()) {
      case MAP:
      case RECORD:
      case ARRAY:
      case UNION:
        return true;
      default:
        return false;
    }
  }


  /**
   * Complex type needs to handle other types inside itself.
   *
   * @param fieldType Complex type's avro schema.
   * @return
   */
  public static boolean isComplexType(FieldType fieldType) {
    return isComplexType(fieldType, true);
  }

  /**
   * Complex type needs to handle other types inside itself.
   *
   * @param fieldType Complex type's avro schema.
   * @return
   */
  public static boolean isComplexType(FieldType fieldType,
                                      boolean checkNullable) {
    if (checkNullable && fieldType.getNullable()) {
      return true;
    }

    switch (fieldType.getTypeName()) {
      case MAP:
      case ROW:
      case ARRAY:
        return true;
      default:
        return false;
    }
  }

  /**
   * Define given schema is stringable or not.
   *
   * @param schema avro schema.
   * @return
   */
  public static boolean isStringable(Schema schema) {
    if (!Schema.Type.STRING.equals(schema.getType())) {
      throw new SchemaAssistantException("String schema expected!");
    }
    return schema.getProp(SpecificData.CLASS_PROP) != null;
  }

  /**
   * Generate a schema name for given schema. We uses this for method name creation.
   * @param schema avro schema.
   * @return
   */
  public static String getSchemaName(Schema schema) {
    String schemaName = null;
    switch (schema.getType()) {
      case RECORD:
        schemaName = schema.getFullName();
        break;
      case ARRAY:
        schemaName = schema.getName() + SEP + getSchemaName(schema.getElementType());
        break;
      case MAP:
        schemaName = schema.getName() + SEP + getSchemaName(schema.getValueType());
        break;
      case UNION:
        schemaName = schema.getName();
        for (Schema option : schema.getTypes()) {
          schemaName += SEP + getSchemaName(option);
        }
        break;
      default:
        schemaName = schema.getName();
    }

    return schemaName;
  }

  /**
   * Generate a field type name for given field type. We uses this for method name creation.
   * @param field beam schema field.
   * @return
   */
  public static String getFieldName(Field field) {
    return getFieldName(field, true);
  }

  /**
   * Generate a field type name for given field type. We uses this for method name creation.
   * @param field beam schema field.
   * @param checkNullable a flag indicate whether check if the field type is nullable.
   * @return
   */
  public static String getFieldName(Field field,
                                    boolean checkNullable) {
    String fieldName;
    FieldType fieldType = field.getType();
    if (checkNullable && fieldType.getNullable()) {
      fieldName = "null" + SEP + getFieldName(field, false);
    } else {
      switch (fieldType.getTypeName()) {
        case ROW:
          fieldName = fieldType.getMetadataString("name");
          fieldName = (fieldName.isEmpty()) ? field.getName() : fieldName;
          break;
        case ARRAY:
          FieldType elementType = fieldType.getCollectionElementType();
          String elementFieldName = "element_" + field.getName();
          Field elementField = Field.of(elementFieldName, elementType);
          fieldName = "array" + SEP
                  + SchemaCodeGeneratorHelper.getFieldName(elementField);
          break;
        case LOGICAL_TYPE:
          switch (fieldType.getLogicalType().getIdentifier()) {
            case FixedBytes.IDENTIFIER:
              return SchemaCodeGeneratorHelper.getFieldName(Field.of(field.getName(),
                      FieldType.BYTES));
            case EnumerationType.IDENTIFIER:
            case CharType.IDENTIFIER:
              return SchemaCodeGeneratorHelper.getFieldName(Field.of(field.getName(),
                      FieldType.STRING));
            default:
              throw new SchemaAssistantException(
                    "Unhandled logical type " + fieldType.getLogicalType().getIdentifier());
          }
        default:
          fieldName = fieldType.getTypeName().toString().toLowerCase();
      }
    }
    return fieldName;
  }
}