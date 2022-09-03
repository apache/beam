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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.beam.sdk.schemas.utils.avro.utils.SerDesUtils.getClassName;
import static org.apache.beam.sdk.schemas.utils.avro.utils.SerDesUtils.getSchemaFingerprint;

import com.sun.codemodel.JArray;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JCatchBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JDefinedClass;
import com.sun.codemodel.JDoLoop;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JFieldVar;
import com.sun.codemodel.JForLoop;
import com.sun.codemodel.JInvocation;
import com.sun.codemodel.JMethod;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JPackage;
import com.sun.codemodel.JStatement;
import com.sun.codemodel.JTryBlock;
import com.sun.codemodel.JVar;
import org.apache.beam.sdk.schemas.utils.avro.exceptions.RowSerdesGeneratorException;
import org.apache.beam.sdk.schemas.utils.avro.utils.SerDesUtils;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.parsing.ResolvingGrammarGenerator;
import org.apache.avro.io.parsing.Symbol;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RowDeserializerCodeGenerator<T> extends SerDesBase {

  private static final Logger LOGGER = LoggerFactory.getLogger(RowDeserializerCodeGenerator.class);

  private static final String DECODER = "decoder";
  private static final String REUSE = "reuse";

  private final Schema writer;
  private final Schema reader;
  private JDefinedClass deserializerClass;
  private JFieldVar schemaMapField;
  private JMethod generateBeamSchemasMethod;
  private Map<Long, Schema> schemaMap = new HashMap<>();
  private Map<Long, JVar> schemaVarMap = new HashMap<>();
  private Map<String, JMethod> deserializeMethodMap = new HashMap<>();
  private Map<String, JMethod> skipMethodMap = new HashMap<>();
  private Map<JMethod, Set<Class<? extends Exception>>> exceptionFromMethodMap = new HashMap<>();
  private JFieldVar avroSchemaMapField;


  /**
   * Row Deserializer Code generator class. Based on given schemas it generates Java code.
   *
   * @param writer           the writer's Avro schema
   * @param reader           the reader's Avro schema
   * @param destination      Path for generated java codes.
   * @param classLoader      classLoader
   * @param compileClassPath Path for compiled java classes.
   */
  public RowDeserializerCodeGenerator(Schema writer, Schema reader, File destination,
      ClassLoader classLoader, String compileClassPath) {
    super("deserializer", destination, classLoader, compileClassPath);
    this.writer = writer;
    this.reader = reader;
    LOGGER.warn("RowDeserializerCodeGenerator Constructed.");
  }

  /**
   * Row Deserializer Code generator. Based on given constructor's variable it generates Java Code
   * and load it in classpath then return instance of the generated code.
   *
   * @return {@link RowDeserializer}
   */
  public RowDeserializer<T> generateDeserializer() {
    LOGGER.warn("Start generating code of deserializer");
    String className = getClassName(writer, reader, "RowDeserializer");
    JPackage classPackage = codeModel._package(this.generatedPackageName);
    LOGGER.warn("Start generating code of deserializer: " + className);

    try {
      deserializerClass = classPackage._class(className);

      JVar readerSchemaVar = deserializerClass
          .field(JMod.PRIVATE | JMod.FINAL, Schema.class, "avroSchema");
      JMethod constructor = deserializerClass.constructor(JMod.PUBLIC);
      JVar constructorParam = constructor.param(Schema.class, "avroSchema");
      constructor.body().assign(JExpr.refthis(readerSchemaVar.name()), constructorParam);

      Schema aliasedWriterSchema = Schema.applyAliases(writer, reader);
      Symbol resolvingGrammar = new ResolvingGrammarGenerator()
          .generate(aliasedWriterSchema, reader);
      FieldAction fieldAction = FieldAction
          .fromValues(aliasedWriterSchema.getType(), true, resolvingGrammar);

      schemaMapField = deserializerClass.field(JMod.PRIVATE,
          codeModel.ref(Map.class).narrow(Long.class)
              .narrow(org.apache.beam.sdk.schemas.Schema.class), "beamSchemaMap");
      avroSchemaMapField = deserializerClass.field(JMod.PRIVATE,
          codeModel.ref(Map.class).narrow(Long.class)
              .narrow(Schema.class), "avroSchemaMap");
      generateBeamSchemasMethod = deserializerClass
          .method(JMod.PRIVATE | JMod.FINAL, void.class, "generateBeamSchemas");
      constructor.body().invoke(generateBeamSchemasMethod);
      generateBeamSchemasMethod.body().assign(schemaMapField,
          JExpr._new(codeModel.ref(HashMap.class).narrow(Long.class)
              .narrow(org.apache.beam.sdk.schemas.Schema.class)));
      generateBeamSchemasMethod.body().assign(avroSchemaMapField,
          JExpr._new(codeModel.ref(HashMap.class).narrow(Long.class)
              .narrow(Schema.class)));
      registerSchema(aliasedWriterSchema, readerSchemaVar);

      JClass returnType = codeModel.ref(Row.class);

      deserializerClass._implements(codeModel.ref(RowDeserializer.class).narrow(returnType));
      JMethod deserializeMethod = deserializerClass.method(JMod.PUBLIC, returnType, "deserialize");

      JBlock topLevelDeserializeBlock = new JBlock();

      switch (aliasedWriterSchema.getType()) {
        case RECORD:
          LOGGER.info("top-level writer schema: record");
          processRecord(readerSchemaVar, aliasedWriterSchema.getName(), aliasedWriterSchema, reader,
              topLevelDeserializeBlock, fieldAction, JBlock::_return);
          break;
        case UNION:
          LOGGER.info("top-level writer schema: union");
          processUnion(readerSchemaVar, aliasedWriterSchema.getName(), aliasedWriterSchema, reader,
              topLevelDeserializeBlock, fieldAction, JBlock::_return);
          break;
        default:
          throw new RowSerdesGeneratorException(
              "Incorrect top-level writer schema: " + aliasedWriterSchema.getType());
      }

      if (schemaCodeGeneratorHelper.getExceptionsFromStringable().isEmpty()) {
        assignBlockToBody(deserializeMethod, topLevelDeserializeBlock);
      } else {
        JTryBlock tryBlock = deserializeMethod.body()._try();
        assignBlockToBody(tryBlock, topLevelDeserializeBlock);

        for (Class<? extends Exception> classException : schemaCodeGeneratorHelper
            .getExceptionsFromStringable()) {
          JCatchBlock catchBlock = tryBlock._catch(codeModel.ref(classException));
          JVar exceptionVar = catchBlock.param("e");
          catchBlock.body()
              ._throw(JExpr._new(codeModel.ref(AvroRuntimeException.class)).arg(exceptionVar));
        }
      }

      if (aliasedWriterSchema.getType() == Type.UNION) {
        deserializeMethod.body()._return(JExpr._null());
      }

      deserializeMethod._throws(codeModel.ref(IOException.class));
      deserializeMethod.param(Row.class, REUSE);
      deserializeMethod.param(Decoder.class, DECODER);

      //codeModel.build(destination);

      //LOGGER.error("Start Compiling Generated Code");
      Class<RowDeserializer<T>> clazz = compileClass(className,
          schemaCodeGeneratorHelper.getUsedFullyQualifiedClassNameSet());
      return clazz.getConstructor(Schema.class).newInstance(reader);
    } catch (Exception e) {
      LOGGER.error("RowDeserializerCodeGenerator Exception: ", e);
    }

    return null;
  }

  private void processRecord(JVar recordSchemaVar, String recordName,
      final Schema recordWriterSchema,
      final Schema recordReaderSchema, JBlock parentBody, FieldAction recordAction,
      BiConsumer<JBlock, JExpression> putRecordIntoParent) {

    ListIterator<Symbol> actionIterator = actionIterator(recordAction);

    if (methodAlreadyDefined(recordWriterSchema, recordAction.getShouldRead())) {
      JMethod method = getMethod(recordWriterSchema, recordAction.getShouldRead());
      updateActualExceptions(method);
      JExpression readingExpression = JExpr.invoke(method).arg(JExpr.direct(DECODER));
      if (recordAction.getShouldRead()) {
        putRecordIntoParent.accept(parentBody, readingExpression);
      } else {
        parentBody.add((JStatement) readingExpression);
      }

      // seek through actionIterator
      for (Schema.Field field : recordWriterSchema.getFields()) {
        FieldAction action = seekFieldAction(recordAction.getShouldRead(), field, actionIterator);
        if (action.getSymbol() == END_SYMBOL) {
          break;
        }
      }
      if (!recordAction.getShouldRead()) {
        return;
      }
      // seek through actionIterator also for default values
      Set<String> fieldNamesSet = recordWriterSchema.getFields()
          .stream().map(Schema.Field::name).collect(Collectors.toSet());
      for (Schema.Field readerField : recordReaderSchema.getFields()) {
        if (!fieldNamesSet.contains(readerField.name())) {
          forwardToExpectedDefault(actionIterator);
          seekFieldAction(true, readerField, actionIterator);
        }
      }
      return;
    }

    JMethod method = createMethod(recordWriterSchema, recordAction.getShouldRead());

    Set<Class<? extends Exception>> exceptionsOnHigherLevel = schemaCodeGeneratorHelper
        .getExceptionsFromStringable();
    schemaCodeGeneratorHelper.setExceptionsFromStringable(exceptionsOnHigherLevel);
    schemaCodeGeneratorHelper.resetExceptionsFromStringable();

    if (recordAction.getShouldRead()) {
      putRecordIntoParent.accept(parentBody, JExpr.invoke(method).arg(JExpr.direct(DECODER)));
    } else {
      parentBody.invoke(method).arg(JExpr.direct(DECODER));
    }

    final JBlock methodBody = method.body();

    final JVar result;
    if (recordAction.getShouldRead()) {
      JArray newRecord = JExpr.newArray(codeModel.ref(Object.class), schemaMapField.invoke("get")
          .arg(JExpr.lit(getSchemaFingerprint(recordWriterSchema)))
          .invoke("getFieldCount"));
      result = methodBody
          .decl(codeModel.ref(Object.class).array(), getUniqueName(recordName), newRecord);
    } else {
      result = null;
    }

    for (Schema.Field field : recordWriterSchema.getFields()) {

      FieldAction action = seekFieldAction(recordAction.getShouldRead(), field, actionIterator);
      if (action.getSymbol() == END_SYMBOL) {
        break;
      }

      Schema readerFieldSchema = null;
      JVar fieldSchemaVar = null;
      BiConsumer<JBlock, JExpression> addExpressionInRecordArray = null;
      if (action.getShouldRead()) {
        Schema.Field readerField = recordReaderSchema.getField(field.name());
        readerFieldSchema = readerField.schema();
        final int readerFieldPos = readerField.pos();
        addExpressionInRecordArray = (block, expression) -> block
            .assign(result.component(JExpr.lit(readerFieldPos)), expression);
        fieldSchemaVar = declareSchemaVar(field.schema(), field.name(),
            recordSchemaVar.invoke("getField").arg(field.name()).invoke("schema"));
      }

      if (SchemaCodeGeneratorHelper.isComplexType(field.schema())) {
        processComplexType(fieldSchemaVar, field.name(), field.schema(), readerFieldSchema,
            methodBody, action, addExpressionInRecordArray);
      } else {
        // to preserve reader string specific options use reader field schema
        if (action.getShouldRead() && Type.STRING.equals(field.schema().getType())) {
          processSimpleType(readerFieldSchema, methodBody, action, addExpressionInRecordArray);
        } else {
          processSimpleType(field.schema(), methodBody, action, addExpressionInRecordArray);
        }
      }
    }

    // Handle default values
    if (recordAction.getShouldRead()) {
      Set<String> fieldNamesSet = recordWriterSchema.getFields().stream().map(Schema.Field::name)
          .collect(Collectors.toSet());
      for (Schema.Field readerField : recordReaderSchema.getFields()) {
        if (!fieldNamesSet.contains(readerField.name())) {
          forwardToExpectedDefault(actionIterator);
          seekFieldAction(true, readerField, actionIterator);
          JVar schemaVar = declareSchemaVariableForRecordField(readerField.name(),
              readerField.schema(),
              recordSchemaVar);
          JExpression value = parseDefaultValue(readerField.schema(), readerField.defaultValue(),
              methodBody,
              schemaVar, readerField.name());
          //methodBody.invoke(result, "add").arg(JExpr.lit(readerField.pos())).arg(value);
          methodBody.assign(result.component(JExpr.lit(readerField.pos())), value);
        }
      }
    }

    if (recordAction.getShouldRead()) {
      JInvocation resultList = codeModel.ref(Arrays.class).staticInvoke("asList").arg(result);
      JClass recordClass = schemaCodeGeneratorHelper.classFromSchema(recordWriterSchema);
      JInvocation newRow = recordClass.staticInvoke("withSchema")
          .arg(schemaMapField.invoke("get")
              .arg(JExpr.lit(getSchemaFingerprint(recordWriterSchema))))
          .invoke("attachValues").arg(resultList);
      methodBody._return(newRow);
    }
    exceptionFromMethodMap.put(method, schemaCodeGeneratorHelper.getExceptionsFromStringable());
    schemaCodeGeneratorHelper.setExceptionsFromStringable(exceptionsOnHigherLevel);
    updateActualExceptions(method);
  }

  private void processComplexType(JVar fieldSchemaVar, String name, Schema schema,
      Schema readerFieldSchema,
      JBlock methodBody, FieldAction action,
      BiConsumer<JBlock, JExpression> putExpressionIntoParent) {
    switch (schema.getType()) {
      case RECORD:
        processRecord(fieldSchemaVar, schema.getName(), schema, readerFieldSchema, methodBody,
            action,
            putExpressionIntoParent);
        break;
      case ARRAY:
        processArray(fieldSchemaVar, name, schema, readerFieldSchema, methodBody, action,
            putExpressionIntoParent);
        break;
      case MAP:
        processMap(fieldSchemaVar, name, schema, readerFieldSchema, methodBody, action,
            putExpressionIntoParent);
        break;
      case UNION:
        processUnion(fieldSchemaVar, name, schema, readerFieldSchema, methodBody, action,
            putExpressionIntoParent);
        break;
      default:
        throw new RowSerdesGeneratorException("Incorrect complex type: " + action.getType());
    }
  }

  private void processSimpleType(Schema schema, JBlock methodBody, FieldAction action,
      BiConsumer<JBlock, JExpression> putExpressionIntoParent) {
    switch (schema.getType()) {
      case ENUM:
        processEnum(schema, methodBody, action, putExpressionIntoParent);
        break;
      case FIXED:
        processFixed(schema, methodBody, action, putExpressionIntoParent);
        break;
      default:
        processPrimitive(schema, methodBody, action, putExpressionIntoParent);
    }
  }

  private JExpression parseDefaultValue(Schema schema, JsonNode defaultValue, JBlock body,
      JVar schemaVar, String fieldName) {
    Type schemaType = schema.getType();
    // The default value of union is of the first defined type
    if (Type.UNION.equals(schemaType)) {
      schema = schema.getTypes().get(0);
      schemaType = schema.getType();
      JInvocation optionSchemaExpression = schemaVar.invoke("getTypes").invoke("get")
          .arg(JExpr.lit(0));
      schemaVar = declareSchemaVar(schema, fieldName, optionSchemaExpression);
    }
    // And default value of null is always null
    if (Type.NULL.equals(schemaType)) {
      return JExpr._null();
    }

    if (SchemaCodeGeneratorHelper.isComplexType(schema)) {
      JClass defaultValueClass = schemaCodeGeneratorHelper.classFromSchema(schema, false);
      JInvocation valueInitializationExpr = JExpr._new(defaultValueClass);

      JVar valueVar;
      switch (schemaType) {
        case RECORD:
          valueInitializationExpr = valueInitializationExpr.arg(getSchemaExpr(schema));
          valueVar = body.decl(defaultValueClass, getUniqueName("default" + schema.getName()),
              valueInitializationExpr);
          for (Iterator<Entry<String, JsonNode>> it = defaultValue.getFields();
              it.hasNext(); ) {
            Entry<String, JsonNode> subFieldEntry = it.next();
            Schema.Field subField = schema.getField(subFieldEntry.getKey());

            JVar fieldSchemaVar = declareSchemaVariableForRecordField(subField.name(),
                subField.schema(),
                schemaVar);
            JExpression fieldValue = parseDefaultValue(subField.schema(), subFieldEntry.getValue(),
                body,
                fieldSchemaVar, subField.name());
            body.invoke(valueVar, "put").arg(JExpr.lit(subField.pos())).arg(fieldValue);
          }
          break;
        case ARRAY:
          valueInitializationExpr = valueInitializationExpr
              .arg(JExpr.lit(defaultValue.size())).arg(getSchemaExpr(schema));
          JVar elementSchemaVar = declareSchemaVar(schema.getElementType(), "defaultElementSchema",
              schemaVar.invoke("getElementType"));

          valueVar = body
              .decl(defaultValueClass, getUniqueName("defaultArray"), valueInitializationExpr);

          for (JsonNode arrayElementValue : defaultValue) {
            JExpression arrayElementExpression = parseDefaultValue(schema.getElementType(),
                arrayElementValue,
                body, elementSchemaVar, "arrayValue");
            body.invoke(valueVar, "add").arg(arrayElementExpression);
          }
          break;
        case MAP:
          JVar mapValueSchemaVar = declareSchemaVar(schema.getValueType(), "defaultMapValueSchema",
              schemaVar.invoke("getValueType"));

          valueVar = body
              .decl(defaultValueClass, getUniqueName("defaultMap"), valueInitializationExpr);

          for (Iterator<Entry<String, JsonNode>> it = defaultValue.getFields();
              it.hasNext(); ) {
            Entry<String, JsonNode> mapEntry = it.next();
            JExpression mapKeyExpr = JExpr.lit(mapEntry.getKey());
            JExpression mapEntryValueExpression = parseDefaultValue(schema.getValueType(),
                mapEntry.getValue(),
                body,
                mapValueSchemaVar, "mapElement");
            body.invoke(valueVar, "put").arg(mapKeyExpr).arg(mapEntryValueExpression);
          }
          break;
        default:
          throw new RowSerdesGeneratorException("Incorrect schema type in default value!");
      }
      return valueVar;
    } else {
      switch (schemaType) {
        case ENUM:
          return JExpr.lit(defaultValue.getTextValue());
        case FIXED:
          JArray fixedBytesArray = JExpr.newArray(codeModel.BYTE);
          for (char b : defaultValue.getTextValue().toCharArray()) {
            fixedBytesArray.add(JExpr.lit((byte) b));
          }
          return schemaCodeGeneratorHelper.getFixedValue(schema, fixedBytesArray);
        case BYTES:
          JArray bytesArray = JExpr.newArray(codeModel.BYTE);
          for (byte b : defaultValue.getTextValue().getBytes(UTF_8)) {
            bytesArray.add(JExpr.lit(b));
          }
          return codeModel.ref(ByteBuffer.class).staticInvoke("wrap").arg(bytesArray);
        case STRING:
          return JExpr.lit(defaultValue.getTextValue());
        case INT:
          return JExpr.lit(defaultValue.getIntValue());
        case LONG:
          return JExpr.lit(defaultValue.getLongValue());
        case FLOAT:
          return JExpr.lit((float) defaultValue.getDoubleValue());
        case DOUBLE:
          return JExpr.lit(defaultValue.getDoubleValue());
        case BOOLEAN:
          return JExpr.lit(defaultValue.getBooleanValue());
        case NULL:
        default:
          throw new RowSerdesGeneratorException("Incorrect schema type in default value!");
      }
    }
  }

  private void processUnion(JVar unionSchemaVar, final String name, final Schema unionSchema,
      final Schema readerUnionSchema, JBlock body, FieldAction action,
      BiConsumer<JBlock, JExpression> putValueIntoParent) {
    JVar unionIndex = body.decl(codeModel.INT, getUniqueName("unionIndex"),
        JExpr.direct(DECODER + ".readIndex()"));
    JConditional ifBlock = null;
    for (int i = 0; i < unionSchema.getTypes().size(); i++) {
      Schema optionSchema = unionSchema.getTypes().get(i);
      Schema readerOptionSchema = null;
      FieldAction unionAction;

      if (Type.NULL.equals(optionSchema.getType())) {
        JBlock nullReadBlock = body._if(unionIndex.eq(JExpr.lit(i)))._then().block();
        nullReadBlock.directStatement(DECODER + ".readNull();");
        if (action.getShouldRead()) {
          putValueIntoParent.accept(nullReadBlock, JExpr._null());
        }
        continue;
      }

      if (action.getShouldRead()) {
        readerOptionSchema = readerUnionSchema.getTypes().get(i);
        Symbol.Alternative alternative = null;
        if (action.getSymbol() instanceof Symbol.Alternative) {
          alternative = (Symbol.Alternative) action.getSymbol();
        } else if (action.getSymbol().production != null) {
          for (Symbol symbol : action.getSymbol().production) {
            if (symbol instanceof Symbol.Alternative) {
              alternative = (Symbol.Alternative) symbol;
              break;
            }
          }
        }

        if (alternative == null) {
          throw new RowSerdesGeneratorException("Unable to determine action for field: " + name);
        }

        Symbol.UnionAdjustAction unionAdjustAction = (Symbol.UnionAdjustAction)
            alternative.symbols[i].production[0];
        unionAction = FieldAction.fromValues(optionSchema.getType(), action.getShouldRead(),
            unionAdjustAction.symToParse);
      } else {
        unionAction = FieldAction.fromValues(optionSchema.getType(), false, EMPTY_SYMBOL);
      }

      JExpression condition = unionIndex.eq(JExpr.lit(i));
      ifBlock = ifBlock != null ? ifBlock._elseif(condition) : body._if(condition);
      final JBlock thenBlock = ifBlock._then();

      JVar optionSchemaVar = null;
      if (unionAction.getShouldRead()) {
        JInvocation optionSchemaExpression = unionSchemaVar.invoke("getTypes").invoke("get")
            .arg(JExpr.lit(i));
        optionSchemaVar = declareSchemaVar(optionSchema, name + "OptionSchema",
            optionSchemaExpression);
      }

      if (SchemaCodeGeneratorHelper.isComplexType(optionSchema)) {
        String optionName = name + "Option";
        if (Type.UNION.equals(optionSchema.getType())) {
          throw new RowSerdesGeneratorException("Union cannot be sub-type of union!");
        }
        processComplexType(optionSchemaVar, optionName, optionSchema, readerOptionSchema, thenBlock,
            unionAction, putValueIntoParent);
      } else {
        // to preserve reader string specific options use reader option schema
        if (action.getShouldRead() && Type.STRING.equals(optionSchema.getType())) {
          processSimpleType(readerOptionSchema, thenBlock, unionAction, putValueIntoParent);

        } else {
          processSimpleType(optionSchema, thenBlock, unionAction, putValueIntoParent);
        }
      }
    }
  }

  private void processArray(JVar arraySchemaVar, final String name, final Schema arraySchema,
      final Schema readerArraySchema, JBlock parentBody, FieldAction action,
      BiConsumer<JBlock, JExpression> putArrayIntoParent) {

    if (action.getShouldRead()) {
      Symbol valuesActionSymbol = null;
      for (Symbol symbol : action.getSymbol().production) {
        if (Symbol.Kind.REPEATER.equals(symbol.kind)
            && "array-end".equals(getSymbolPrintName(((Symbol.Repeater) symbol).end))) {
          valuesActionSymbol = symbol;
          break;
        }
      }

      if (valuesActionSymbol == null) {
        throw new RowSerdesGeneratorException("Unable to determine action for array: " + name);
      }

      action = FieldAction
          .fromValues(arraySchema.getElementType().getType(), action.getShouldRead(),
              valuesActionSymbol);
    } else {
      action = FieldAction.fromValues(arraySchema.getElementType().getType(), false, EMPTY_SYMBOL);
    }

    final JVar arrayVar =
        action.getShouldRead() ? declareValueVar(name, readerArraySchema, parentBody) : null;
    JVar chunkLen = parentBody.decl(codeModel.LONG, getUniqueName("chunkLen"),
        JExpr.direct(DECODER + ".readArrayStart()"));

    JConditional conditional = parentBody._if(chunkLen.gt(JExpr.lit(0)));
    JBlock ifBlock = conditional._then();

    JClass arrayClass = schemaCodeGeneratorHelper
        .classFromSchema(action.getShouldRead() ? readerArraySchema : arraySchema, false);

    if (action.getShouldRead()) {
      JInvocation newArrayExp = JExpr._new(arrayClass);
      newArrayExp = newArrayExp.arg(JExpr.cast(codeModel.INT, chunkLen));
      ifBlock.assign(arrayVar, newArrayExp);
      JBlock elseBlock = conditional._else();
      elseBlock.assign(arrayVar,
          JExpr._new(arrayClass).arg(JExpr.lit(0)));
    }

    JDoLoop doLoop = ifBlock._do(chunkLen.gt(JExpr.lit(0)));
    JForLoop forLoop = doLoop.body()._for();
    JVar counter = forLoop.init(codeModel.INT, getUniqueName("counter"), JExpr.lit(0));
    forLoop.test(counter.lt(chunkLen));
    forLoop.update(counter.incr());
    JBlock forBody = forLoop.body();

    JVar elementSchemaVar = null;
    BiConsumer<JBlock, JExpression> putValueInArray = null;
    if (action.getShouldRead()) {
      putValueInArray = (block, expression) -> block.invoke(arrayVar, "add").arg(expression);
      elementSchemaVar = declareSchemaVar(arraySchema.getElementType(), name + "ArrayElemSchema",
          arraySchemaVar.invoke("getElementType"));
    }

    if (SchemaCodeGeneratorHelper.isComplexType(arraySchema.getElementType())) {
      String elemName = name + "Elem";
      Schema readerArrayElementSchema =
          action.getShouldRead() ? readerArraySchema.getElementType() : null;
      processComplexType(elementSchemaVar, elemName, arraySchema.getElementType(),
          readerArrayElementSchema,
          forBody, action, putValueInArray);
    } else {
      // to preserve reader string specific options use reader array schema
      if (action.getShouldRead() && Type.STRING
          .equals(arraySchema.getElementType().getType())) {
        processSimpleType(readerArraySchema.getElementType(), forBody, action, putValueInArray);
      } else {
        processSimpleType(arraySchema.getElementType(), forBody, action, putValueInArray);
      }
    }
    doLoop.body().assign(chunkLen, JExpr.direct(DECODER + ".arrayNext()"));

    if (action.getShouldRead()) {
      putArrayIntoParent.accept(parentBody, arrayVar);
    }
  }

  private void processMap(JVar mapSchemaVar, final String name, final Schema mapSchema,
      final Schema readerMapSchema,
      JBlock parentBody, FieldAction action, BiConsumer<JBlock, JExpression> putMapIntoParent) {

    if (action.getShouldRead()) {
      Symbol valuesActionSymbol = null;
      for (Symbol symbol : action.getSymbol().production) {
        if (Symbol.Kind.REPEATER.equals(symbol.kind)
            && "map-end".equals(getSymbolPrintName(((Symbol.Repeater) symbol).end))) {
          valuesActionSymbol = symbol;
          break;
        }
      }

      if (valuesActionSymbol == null) {
        throw new RowSerdesGeneratorException("unable to determine action for map: " + name);
      }

      action = FieldAction.fromValues(mapSchema.getValueType().getType(), action.getShouldRead(),
          valuesActionSymbol);
    } else {
      action = FieldAction.fromValues(mapSchema.getValueType().getType(), false, EMPTY_SYMBOL);
    }

    final JVar mapVar =
        action.getShouldRead() ? declareValueVar(name, readerMapSchema, parentBody) : null;
    JVar chunkLen = parentBody.decl(codeModel.LONG, getUniqueName("chunkLen"),
        JExpr.direct(DECODER + ".readMapStart()"));

    JConditional conditional = parentBody._if(chunkLen.gt(JExpr.lit(0)));
    JBlock ifBlock = conditional._then();

    if (action.getShouldRead()) {
      ifBlock.assign(mapVar, JExpr._new(
          schemaCodeGeneratorHelper.classFromSchema(readerMapSchema, false)));
      JBlock elseBlock = conditional._else();
      elseBlock.assign(mapVar, codeModel.ref(Collections.class).staticInvoke("emptyMap"));
    }

    JDoLoop doLoop = ifBlock._do(chunkLen.gt(JExpr.lit(0)));
    JForLoop forLoop = doLoop.body()._for();
    JVar counter = forLoop.init(codeModel.INT, getUniqueName("counter"), JExpr.lit(0));
    forLoop.test(counter.lt(chunkLen));
    forLoop.update(counter.incr());
    JBlock forBody = forLoop.body();

    JClass keyClass = schemaCodeGeneratorHelper
        .keyClassFromMapSchema(action.getShouldRead() ? readerMapSchema : mapSchema);
    JExpression keyValueExpression = JExpr.direct(DECODER + ".readString()");

    JVar key = forBody.decl(keyClass, getUniqueName("key"), keyValueExpression);
    JVar mapValueSchemaVar = null;
    if (action.getShouldRead()) {
      mapValueSchemaVar = declareSchemaVar(mapSchema.getValueType(), name + "MapValueSchema",
          mapSchemaVar.invoke("getValueType"));
    }

    BiConsumer<JBlock, JExpression> putValueInMap = null;
    if (action.getShouldRead()) {
      putValueInMap = (block, expression) -> block.invoke(mapVar, "put").arg(key).arg(expression);
    }

    if (SchemaCodeGeneratorHelper.isComplexType(mapSchema.getValueType())) {
      String valueName = name + "Value";
      Schema readerMapValueSchema = null;
      if (action.getShouldRead()) {
        readerMapValueSchema = readerMapSchema.getValueType();
      }
      processComplexType(mapValueSchemaVar, valueName, mapSchema.getValueType(),
          readerMapValueSchema, forBody,
          action, putValueInMap);
    } else {
      // to preserve reader string specific options use reader map schema
      if (action.getShouldRead() && Type.STRING.equals(mapSchema.getValueType().getType())) {
        processSimpleType(readerMapSchema.getValueType(), forBody, action, putValueInMap);
      } else {
        processSimpleType(mapSchema.getValueType(), forBody, action, putValueInMap);
      }
    }
    doLoop.body().assign(chunkLen, JExpr.direct(DECODER + ".mapNext()"));

    if (action.getShouldRead()) {
      putMapIntoParent.accept(parentBody, mapVar);
    }
  }

  private void processFixed(final Schema schema, JBlock body, FieldAction action,
      BiConsumer<JBlock, JExpression> putFixedIntoParent) {
    if (action.getShouldRead()) {
      JVar fixedBuffer = body.decl(codeModel.ref(byte[].class), getUniqueName(schema.getName()))
          .init(JExpr.direct(" new byte[" + schema.getFixedSize() + "]"));

      body.directStatement(DECODER + ".readFixed(" + fixedBuffer.name() + ");");

      putFixedIntoParent.accept(body, fixedBuffer);
    } else {
      body.directStatement(DECODER + ".skipFixed(" + schema.getFixedSize() + ");");
    }
  }

  private void processEnum(final Schema schema, final JBlock body, FieldAction action,
      BiConsumer<JBlock, JExpression> putEnumIntoParent) {

    if (action.getShouldRead()) {

      Symbol.EnumAdjustAction enumAdjustAction = null;
      if (action.getSymbol() instanceof Symbol.EnumAdjustAction) {
        enumAdjustAction = (Symbol.EnumAdjustAction) action.getSymbol();
      } else {
        for (Symbol symbol : action.getSymbol().production) {
          if (symbol instanceof Symbol.EnumAdjustAction) {
            enumAdjustAction = (Symbol.EnumAdjustAction) symbol;
          }
        }
      }

      boolean enumOrderCorrect = true;
      for (int i = 0; i < enumAdjustAction.adjustments.length; i++) {
        Object adjustment = enumAdjustAction.adjustments[i];
        if (adjustment instanceof String) {
          throw new RowSerdesGeneratorException(
              schema.getName() + " enum label impossible to deserialize: " + adjustment.toString());
        } else if (!adjustment.equals(i)) {
          enumOrderCorrect = false;
        }
      }

      JExpression newEnum;
      JExpression enumValueExpr = JExpr.direct(DECODER + ".readEnum()");

      if (enumOrderCorrect) {
        newEnum = schemaCodeGeneratorHelper
            .getEnumValueByIndex(schema, enumValueExpr, getAvroSchemaExpr(schema));
      } else {
        JVar enumIndex = body.decl(codeModel.INT, getUniqueName("enumIndex"), enumValueExpr);
        JClass enumClass = schemaCodeGeneratorHelper.classFromSchema(schema);
        newEnum = body.decl(enumClass, getUniqueName("enumValue"), JExpr._null());

        for (int i = 0; i < enumAdjustAction.adjustments.length; i++) {
          JExpression ithVal = schemaCodeGeneratorHelper
              .getEnumValueByIndex(schema, JExpr.lit((Integer) enumAdjustAction.adjustments[i]),
                  getAvroSchemaExpr(schema));
          body._if(enumIndex.eq(JExpr.lit(i)))._then().assign((JVar) newEnum, ithVal);
        }
      }
      putEnumIntoParent.accept(body, newEnum);
    } else {
      body.directStatement(DECODER + ".readEnum();");
    }

  }

  private void processPrimitive(final Schema schema, JBlock body, FieldAction action,
      BiConsumer<JBlock, JExpression> putValueIntoParent) {

    String readFunction;
    switch (schema.getType()) {
      case STRING:
        if (action.getShouldRead()) {
          readFunction = "readString()";
        } else {
          readFunction = "skipString()";
        }
        break;
      case BYTES:
        readFunction = "readBytes(null)";
        break;
      case INT:
        readFunction = "readInt()";
        break;
      case LONG:
        readFunction = "readLong()";
        break;
      case FLOAT:
        readFunction = "readFloat()";
        break;
      case DOUBLE:
        readFunction = "readDouble()";
        break;
      case BOOLEAN:
        readFunction = "readBoolean()";
        break;
      default:
        throw new RowSerdesGeneratorException(
            "Unsupported primitive schema of type: " + schema.getType());
    }

    JExpression primitiveValueExpression = JExpr.direct("decoder." + readFunction);
    if (action.getShouldRead()) {
      if (schema.getType().equals(Type.STRING) && SchemaCodeGeneratorHelper
          .isStringable(schema)) {
        primitiveValueExpression = JExpr._new(schemaCodeGeneratorHelper.classFromSchema(schema))
            .arg(primitiveValueExpression.invoke("toString"));
      } else if (schema.getType().equals(Type.BYTES)) {
        JVar bbVar = body
            .decl(codeModel.ref(ByteBuffer.class), getUniqueName(Type.BYTES.getName()),
                primitiveValueExpression);
        JArray newByteArray = JExpr.newArray(codeModel.BYTE, bbVar.invoke("remaining"));
        JVar byteArrayVar = body
            .decl(codeModel.BYTE.array(), getUniqueName(Type.BYTES.getName()), newByteArray);
        body.add(bbVar.invoke("duplicate").invoke("get").arg(byteArrayVar));
        primitiveValueExpression = byteArrayVar;

      }
      putValueIntoParent.accept(body, primitiveValueExpression);
    } else {
      body.directStatement(DECODER + "." + readFunction + ";");
    }
  }

  private JVar declareSchemaVariableForRecordField(final String name, final Schema schema,
      JVar schemaVar) {
    return declareSchemaVar(schema, name + "Field",
        schemaVar.invoke("getField").arg(name).invoke("schema"));
  }

  private JVar declareValueVar(final String name, final Schema schema, JBlock block) {
    if (SchemaCodeGeneratorHelper.isComplexType(schema)) {
      return block.decl(schemaCodeGeneratorHelper.classFromSchema(schema),
          getUniqueName(StringUtils.uncapitalize(name)), JExpr._null());
    } else {
      throw new RowSerdesGeneratorException("Only complex types allowed!");
    }
  }

  private JVar declareSchemaVar(Schema valueSchema, String variableName, JInvocation getValueType) {
    if (SchemaCodeGeneratorHelper.isComplexType(valueSchema) || Type.ENUM
        .equals(valueSchema.getType())) {
      Long schemaId = getSchemaFingerprint(valueSchema);
      if (schemaVarMap.get(schemaId) != null) {
        return schemaVarMap.get(schemaId);
      } else {
        JVar schemaVar = generateBeamSchemasMethod.body().decl(codeModel.ref(Schema.class),
            getUniqueName(StringUtils.uncapitalize(variableName)), getValueType);
        registerSchema(valueSchema, schemaId, schemaVar);
        schemaVarMap.put(schemaId, schemaVar);
        return schemaVar;
      }
    } else {
      return null;
    }
  }

  private void updateActualExceptions(JMethod method) {
    Set<Class<? extends Exception>> exceptionFromMethod = exceptionFromMethodMap.get(method);
    for (Class<? extends Exception> exceptionClass : exceptionFromMethod) {
      method._throws(exceptionClass);
      schemaCodeGeneratorHelper.getExceptionsFromStringable().add(exceptionClass);
    }
  }

  protected static void assignBlockToBody(Object codeContainer, JBlock body) {
    try {
      Field field = codeContainer.getClass().getDeclaredField("body");

      field.setAccessible(true);
      field.set(codeContainer, body);
      field.setAccessible(false);

    } catch (ReflectiveOperationException e) {
      throw new RowSerdesGeneratorException(e);
    }
  }

  private void registerSchema(final Schema writerSchema, JVar schemaVar) {
    registerSchema(writerSchema, SerDesUtils.getSchemaFingerprint(writerSchema), schemaVar);
  }

  private void registerSchema(final Schema writerSchema, long schemaId, JVar schemaVar) {
    if ((Type.RECORD.equals(writerSchema.getType()) && schemaNotRegistered(writerSchema))) {
      schemaMap.put(schemaId, writerSchema);
      generateBeamSchemasMethod.body().invoke(schemaMapField, "put").arg(JExpr.lit(schemaId))
          .arg(codeModel.ref(AvroUtils.class).staticInvoke("toBeamSchema").arg(schemaVar));
    } else if (Type.ENUM.equals(writerSchema.getType()) && schemaNotRegistered(
        writerSchema)) {
      schemaMap.put(schemaId, writerSchema);
      generateBeamSchemasMethod.body().invoke(avroSchemaMapField, "put").arg(JExpr.lit(schemaId))
          .arg(schemaVar);
    }
  }

  private boolean schemaNotRegistered(final Schema schema) {
    return !schemaMap.containsKey(SerDesUtils.getSchemaFingerprint(schema));
  }

  protected ListIterator<Symbol> actionIterator(FieldAction action) {
    ListIterator<Symbol> actionIterator = null;

    if (action.getSymbolIterator() != null) {
      actionIterator = action.getSymbolIterator();
    } else if (action.getSymbol().production != null) {
      actionIterator = Arrays.asList(reverseSymbolArray(action.getSymbol().production))
          .listIterator();
    } else {
      actionIterator = Collections.emptyListIterator();
    }

    while (actionIterator.hasNext()) {
      Symbol symbol = actionIterator.next();

      if (symbol instanceof Symbol.ErrorAction) {
        throw new RowSerdesGeneratorException(((Symbol.ErrorAction) symbol).msg);
      }

      if (symbol instanceof Symbol.FieldOrderAction) {
        break;
      }
    }

    return actionIterator;
  }

  protected static Symbol[] reverseSymbolArray(Symbol[] symbols) {
    Symbol[] reversedSymbols = new Symbol[symbols.length];

    for (int i = 0; i < symbols.length; i++) {
      reversedSymbols[symbols.length - i - 1] = symbols[i];
    }

    return reversedSymbols;
  }

  protected void forwardToExpectedDefault(ListIterator<Symbol> symbolIterator) {
    Symbol symbol;
    while (symbolIterator.hasNext()) {
      symbol = symbolIterator.next();

      if (symbol instanceof Symbol.ErrorAction) {
        throw new RowSerdesGeneratorException(((Symbol.ErrorAction) symbol).msg);
      }

      if (symbol instanceof Symbol.DefaultStartAction) {
        return;
      }
    }
    throw new RowSerdesGeneratorException("DefaultStartAction symbol expected!");
  }

  protected FieldAction seekFieldAction(boolean shouldReadCurrent, Schema.Field field,
      ListIterator<Symbol> symbolIterator) {

    Type type = field.schema().getType();

    if (!shouldReadCurrent) {
      return FieldAction.fromValues(type, false, EMPTY_SYMBOL);
    }

    boolean shouldRead = true;
    Symbol fieldSymbol = END_SYMBOL;

    if (Type.RECORD.equals(type)) {
      if (symbolIterator.hasNext()) {
        fieldSymbol = symbolIterator.next();
        if (fieldSymbol instanceof Symbol.SkipAction) {
          return FieldAction.fromValues(type, false, fieldSymbol);
        } else {
          symbolIterator.previous();
        }
      }
      return FieldAction.fromValues(type, true, symbolIterator);
    }

    while (symbolIterator.hasNext()) {
      Symbol symbol = symbolIterator.next();

      if (symbol instanceof Symbol.ErrorAction) {
        throw new RowSerdesGeneratorException(((Symbol.ErrorAction) symbol).msg);
      }

      if (symbol instanceof Symbol.SkipAction) {
        shouldRead = false;
        fieldSymbol = symbol;
        break;
      }

      if (symbol instanceof Symbol.WriterUnionAction) {
        if (symbolIterator.hasNext()) {
          symbol = symbolIterator.next();

          if (symbol instanceof Symbol.Alternative) {
            shouldRead = true;
            fieldSymbol = symbol;
            break;
          }
        }
      }

      if (symbol.kind == Symbol.Kind.TERMINAL) {
        shouldRead = true;
        if (symbolIterator.hasNext()) {
          symbol = symbolIterator.next();

          if (symbol instanceof Symbol.Repeater) {
            fieldSymbol = symbol;
          } else {
            fieldSymbol = symbolIterator.previous();
          }
        } else if (!symbolIterator.hasNext() && getSymbolPrintName(symbol) != null) {
          fieldSymbol = symbol;
        }
        break;
      }
    }

    return FieldAction.fromValues(type, shouldRead, fieldSymbol);
  }

  protected static String getSymbolPrintName(Symbol symbol) {
    String printName;
    try {
      Field field = symbol.getClass().getDeclaredField("printName");

      field.setAccessible(true);
      printName = (String) field.get(symbol);
      field.setAccessible(false);

    } catch (ReflectiveOperationException e) {
      throw new RowSerdesGeneratorException(e);
    }

    return printName;
  }

  protected static final Symbol EMPTY_SYMBOL = new Symbol(Symbol.Kind.TERMINAL, new Symbol[]{}) {
  };

  protected static final Symbol END_SYMBOL = new Symbol(Symbol.Kind.TERMINAL, new Symbol[]{}) {
  };

  protected static final class FieldAction {

    private Type type;
    private boolean shouldRead;
    private Symbol symbol;
    private ListIterator<Symbol> symbolIterator;

    private FieldAction(Type type, boolean shouldRead, Symbol symbol) {
      this.type = type;
      this.shouldRead = shouldRead;
      this.symbol = symbol;
    }

    private FieldAction(Type type, boolean shouldRead, ListIterator<Symbol> symbolIterator) {
      this.type = type;
      this.shouldRead = shouldRead;
      this.symbolIterator = symbolIterator;
    }

    public static FieldAction fromValues(Type type, boolean read, Symbol symbol) {
      return new FieldAction(type, read, symbol);
    }

    public static FieldAction fromValues(Type type, boolean read,
        ListIterator<Symbol> symbolIterator) {
      return new FieldAction(type, read, symbolIterator);
    }

    public Type getType() {
      return type;
    }

    public boolean getShouldRead() {
      return shouldRead;
    }

    public Symbol getSymbol() {
      return symbol;
    }

    public ListIterator<Symbol> getSymbolIterator() {
      return symbolIterator;
    }
  }

  private boolean methodAlreadyDefined(final Schema schema, boolean read) {
    if (!Type.RECORD.equals(schema.getType())) {
      throw new RowSerdesGeneratorException(
          "Methods are defined only for records, not for " + schema.getType());
    }

    return (read ? deserializeMethodMap : skipMethodMap).containsKey(schema.getFullName());
  }

  private JMethod getMethod(final Schema schema, boolean read) {
    if (!Type.RECORD.equals(schema.getType())) {
      throw new RowSerdesGeneratorException(
          "Methods are defined only for records, not for " + schema.getType());
    }
    if (!methodAlreadyDefined(schema, read)) {
      throw new RowSerdesGeneratorException("No method for schema: " + schema.getFullName());
    }
    return (read ? deserializeMethodMap : skipMethodMap).get(schema.getFullName());
  }

  private JMethod createMethod(final Schema schema, boolean read) {
    if (!Type.RECORD.equals(schema.getType())) {
      throw new RowSerdesGeneratorException(
          "Methods are defined only for records, not for " + schema.getType());
    }
    if (methodAlreadyDefined(schema, read)) {
      throw new RowSerdesGeneratorException("Method already exists for: " + schema.getFullName());
    }

    JMethod method = deserializerClass.method(JMod.PUBLIC,
        read ? schemaCodeGeneratorHelper.classFromSchema(schema) : codeModel.VOID,
        getUniqueName("deserialize_" + schema.getName()));

    method._throws(IOException.class);
    method.param(Decoder.class, DECODER);

    (read ? deserializeMethodMap : skipMethodMap).put(schema.getFullName(), method);

    return method;
  }

  private JInvocation getSchemaExpr(Schema schema) {
    return schemaMapField.invoke("get").arg(JExpr.lit(getSchemaFingerprint(schema)));
  }

  private JInvocation getAvroSchemaExpr(Schema schema) {
    return avroSchemaMapField.invoke("get").arg(JExpr.lit(getSchemaFingerprint(schema)));
  }
}
