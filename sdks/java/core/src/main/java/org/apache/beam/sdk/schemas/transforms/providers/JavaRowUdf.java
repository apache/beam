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
package org.apache.beam.sdk.schemas.transforms.providers;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.FieldValueTypeInformation;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.utils.StaticSchemaInference;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;

@SuppressFBWarnings(
    value = "CT_CONSTRUCTOR_THROW",
    justification =
        "Pre-existing finding, not triaged yet."
            + " Making the class final or moving the throwing code"
            + " into a static factory method may fix it.")
public class JavaRowUdf implements Serializable {
  private final Configuration config;
  private final Schema inputSchema;
  private final Schema.FieldType outputType;

  // Transient so we don't have to worry about issues serializing these dynamically created classes.
  // While this is lazily computed, it is always computed on class construction, so any errors
  // should still be caught at construction time, and lazily re-computed before any use.
  private static final ConcurrentHashMap<String, String> DOWNLOADED_JARS =
      new ConcurrentHashMap<>();

  @SuppressFBWarnings("SE_TRANSIENT_FIELD_NOT_RESTORED")
  private transient volatile Function<Row, Object> function;

  // Find or implement the inverse of StaticSchemaInference.fieldFromType
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class Configuration implements Serializable {
    @SchemaFieldDescription("Source code of a java expression in terms of the schema fields.")
    @Nullable
    public abstract String getExpression();

    @SchemaFieldDescription(
        "Source code of a public class implementing Function<Row, T> for some schema-compatible T.")
    @Nullable
    public abstract String getCallable();

    @SchemaFieldDescription("Path to a jar file implementing the function referenced in name.")
    @Nullable
    public abstract String getPath();

    @SchemaFieldDescription(
        "Fully qualified name of either a class implementing Function<Row, T> (e.g. com.pkg.MyFunction), "
            + "or a method taking a single Row argument (e.g. com.pkg.MyClass::methodName). "
            + "If a method is passed, it must either be static or belong to a class with a public nullary constructor.")
    @Nullable
    public abstract String getName();

    public void validate() {
      checkArgument(
          Strings.isNullOrEmpty(getPath()) || !Strings.isNullOrEmpty(getName()),
          "Specifying a path only allows if a name is provided.");
      int totalArgs =
          (Strings.isNullOrEmpty(getExpression()) ? 0 : 1)
              + (Strings.isNullOrEmpty(getCallable()) ? 0 : 1)
              + (Strings.isNullOrEmpty(getName()) ? 0 : 1);
      checkArgument(
          totalArgs == 1, "Exactly one of expression, callable, or name must be provided.");
    }

    public static Configuration.Builder builder() {
      return new AutoValue_JavaRowUdf_Configuration.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Configuration.Builder setExpression(String expression);

      public abstract Configuration.Builder setCallable(String callable);

      public abstract Configuration.Builder setPath(String path);

      public abstract Configuration.Builder setName(String name);

      public abstract Configuration build();
    }
  }

  public JavaRowUdf(Configuration config, Schema inputSchema)
      throws MalformedURLException, ReflectiveOperationException, StringCompiler.CompileException {
    this.config = config;
    this.inputSchema = inputSchema;
    FunctionAndType functionAndType = createFunction(config, inputSchema);
    this.outputType = functionAndType.outputType;
    this.function = functionAndType.function;
  }

  public Schema.FieldType getOutputType() {
    return outputType;
  }

  public Function<Row, Object> getFunction()
      throws MalformedURLException, ReflectiveOperationException, StringCompiler.CompileException {
    Function<Row, Object> fn = function;
    if (fn == null) {
      fn = createFunctionOnly(config, inputSchema);
      function = fn;
    }
    return fn;
  }

  @SuppressWarnings({
    "nullness" // TODO(https://github.com/apache/beam/issues/20497)
  })
  private static Function<Row, Object> createFunctionOnly(Configuration config, Schema inputSchema)
      throws ReflectiveOperationException, StringCompiler.CompileException, MalformedURLException {
    if (!Strings.isNullOrEmpty(config.getExpression())) {
      return createExpressionFunctionOnly(config.getExpression(), inputSchema);
    }
    return createFunction(config, inputSchema).function;
  }

  private static class FunctionAndType {
    public final Schema.FieldType outputType;
    public final Function<Row, Object> function;

    public FunctionAndType(Function<Row, Object> function) {
      this(outputOf(function), function);
    }

    public FunctionAndType(Type outputType, Function<Row, Object> function) {
      this(TypeDescriptor.of(outputType), function);
    }

    public FunctionAndType(TypeDescriptor<?> outputType, Function<Row, Object> function) {
      this(
          StaticSchemaInference.fieldFromType(outputType, new EmptyFieldValueTypeSupplier()),
          function);
    }

    public FunctionAndType(Schema.FieldType outputType, Function<Row, Object> function) {
      this.outputType = outputType;
      this.function = function;
    }

    public static <InputT, OutputT> TypeDescriptor<OutputT> outputOf(Function<InputT, OutputT> fn) {
      return TypeDescriptors.extractFromTypeParameters(
          fn,
          Function.class,
          new TypeDescriptors.TypeVariableExtractor<Function<InputT, OutputT>, OutputT>() {});
    }
  }

  @SuppressWarnings({
    "nullness" // TODO(https://github.com/apache/beam/issues/20497)
  })
  private static FunctionAndType createFunction(Configuration config, Schema inputSchema)
      throws ReflectiveOperationException, StringCompiler.CompileException, MalformedURLException {
    config.validate();
    if (!Strings.isNullOrEmpty(config.getExpression())) {
      return createFunctionFromExpression(config.getExpression(), inputSchema);
    } else if (!Strings.isNullOrEmpty(config.getCallable())) {
      return createFuctionFromCallable(config.getCallable());
    } else if (!Strings.isNullOrEmpty(config.getName())) {
      return createFunctionFromName(config.getName(), config.getPath());
    } else {
      throw new UnsupportedOperationException(config.toString());
    }
  }

  private static boolean containsIdentifier(String expression, String name) {
    int nameLen = name.length();
    if (nameLen == 0) {
      return false;
    }
    int from = 0;
    int exprLen = expression.length();
    while (true) {
      int idx = expression.indexOf(name, from);
      if (idx == -1) {
        return false;
      }
      boolean leftBoundary =
          idx == 0 || !Character.isJavaIdentifierPart(expression.charAt(idx - 1));
      int end = idx + nameLen;
      boolean rightBoundary =
          end == exprLen || !Character.isJavaIdentifierPart(expression.charAt(end));
      if (leftBoundary && rightBoundary) {
        return true;
      }
      from = idx + 1;
    }
  }

  private static Map<String, Type> referencedFieldTypes(String expression, Schema inputSchema) {
    Map<String, Type> fieldTypes = new LinkedHashMap<>();
    for (Schema.Field field : inputSchema.getFields()) {
      String name = field.getName();
      if (containsIdentifier(expression, name)) {
        fieldTypes.put(name, typeFromFieldType(field.getType()));
      }
    }
    return fieldTypes;
  }

  @SuppressWarnings("unchecked")
  private static Function<Row, Object> compileExpressionFunction(
      String expression, Schema inputSchema, Map<String, Type> fieldTypes)
      throws StringCompiler.CompileException, ReflectiveOperationException {
    StringBuilder source = new StringBuilder();
    source.append("import java.util.function.Function;\n");
    source.append("import " + Row.class.getTypeName() + ";\n");
    source.append("public class Eval implements Function<Row, Object> {\n");
    source.append("  public Object apply(Row __row__) {\n");
    for (Map.Entry<String, Type> fieldEntry : fieldTypes.entrySet()) {
      source.append(
          String.format(
              "    %s %s = (%s) __row__.getValue(%s);%n",
              fieldEntry.getValue().getTypeName(),
              fieldEntry.getKey(),
              fieldEntry.getValue().getTypeName(),
              inputSchema.indexOf(fieldEntry.getKey())));
    }
    source.append("    return " + expression + ";\n");
    source.append("  }\n");
    source.append("}\n");
    return (Function<Row, Object>) StringCompiler.getInstance("Eval", source.toString());
  }

  private static Function<Row, Object> createExpressionFunctionOnly(
      String expression, Schema inputSchema)
      throws StringCompiler.CompileException, ReflectiveOperationException {
    if (inputSchema.hasField(expression)) {
      final int ix = inputSchema.indexOf(expression);
      return (Row row) -> row.getValue(ix);
    }
    Map<String, Type> fieldTypes = referencedFieldTypes(expression, inputSchema);
    return compileExpressionFunction(expression, inputSchema, fieldTypes);
  }

  private static FunctionAndType createFunctionFromExpression(String expression, Schema inputSchema)
      throws StringCompiler.CompileException, ReflectiveOperationException {
    if (inputSchema.hasField(expression)) {
      final int ix = inputSchema.indexOf(expression);
      return new FunctionAndType(
          inputSchema.getField(expression).getType(), (Row row) -> row.getValue(ix));
    } else {
      Map<String, Type> fieldTypes = referencedFieldTypes(expression, inputSchema);
      Type type = StringCompiler.guessExpressionType(expression, fieldTypes);
      return new FunctionAndType(
          type, compileExpressionFunction(expression, inputSchema, fieldTypes));
    }
  }

  @SuppressWarnings({
    "nullness" // TODO(https://github.com/apache/beam/issues/20497)
  })
  private static FunctionAndType createFuctionFromCallable(String callable)
      throws StringCompiler.CompileException, ReflectiveOperationException {
    Matcher matcher =
        Pattern.compile("\\bpublic\\s+class\\s+(\\S+)", Pattern.MULTILINE).matcher(callable);
    Preconditions.checkArgument(matcher.find(), "No public class defined in callable source.");
    return new FunctionAndType(
        (Function<Row, Object>) StringCompiler.getInstance(matcher.group(1), callable.toString()));
  }

  @SuppressWarnings({
    "nullness" // TODO(https://github.com/apache/beam/issues/20497)
  })
  private static FunctionAndType createFunctionFromName(String name, String path)
      throws ReflectiveOperationException, MalformedURLException {
    if (path != null && !new File(path).exists()) {
      final String remotePath = path;
      path =
          DOWNLOADED_JARS.compute(
              remotePath,
              (p, existing) -> {
                if (existing != null && new File(existing).exists()) {
                  return existing;
                }
                try (ReadableByteChannel inChannel =
                    FileSystems.open(FileSystems.matchNewResource(p, false))) {
                  File tmpJar = File.createTempFile("map-to-fields-", ".jar");
                  tmpJar.deleteOnExit();
                  try (FileChannel outChannel =
                      FileChannel.open(tmpJar.toPath(), StandardOpenOption.WRITE)) {
                    ByteStreams.copy(inChannel, outChannel);
                  }
                  return tmpJar.getPath();
                } catch (IOException exn) {
                  throw new RuntimeException(exn);
                }
              });
    }
    ClassLoader classLoader =
        path == null
            ? ClassLoader.getSystemClassLoader()
            : new URLClassLoader(
                new URL[] {new URL("file://" + path)}, ClassLoader.getSystemClassLoader());
    String className, methodName = null;
    if (name.indexOf("::") == -1) {
      className = name;
      methodName = null;
    } else {
      String[] parts = name.split("::", 2);
      className = parts[0];
      methodName = parts[1];
    }
    if (methodName == null) {
      return new FunctionAndType(
          (Function<Row, Object>)
              classLoader.loadClass(className).getDeclaredConstructor().newInstance());
    } else {
      Class<?> clazz = classLoader.loadClass(className);
      Method method = clazz.getMethod(methodName, Row.class);
      boolean isStatic = Modifier.isStatic(method.getModifiers());
      Object base = isStatic ? null : clazz.getDeclaredConstructor().newInstance();
      Function<Row, Object> fn;
      try {
        MethodHandle rawHandle = MethodHandles.lookup().unreflect(method);
        MethodHandle boundHandle = isStatic ? rawHandle : rawHandle.bindTo(base);
        fn =
            (Row row) -> {
              try {
                return boundHandle.invoke(row);
              } catch (RuntimeException | Error e) {
                throw e;
              } catch (Throwable exn) {
                throw new RuntimeException(exn);
              }
            };
      } catch (IllegalAccessException e) {
        fn =
            (Row row) -> {
              try {
                return method.invoke(base, row);
              } catch (ReflectiveOperationException exn) {
                throw new RuntimeException(exn);
              }
            };
      }
      return new FunctionAndType(method.getGenericReturnType(), fn);
    }
  }

  private static class EmptyFieldValueTypeSupplier
      implements org.apache.beam.sdk.schemas.utils.FieldValueTypeSupplier {
    @Override
    public List<FieldValueTypeInformation> get(TypeDescriptor<?> typeDescriptor) {
      return Collections.<FieldValueTypeInformation>emptyList();
    }
  }

  private static final Map<Schema.TypeName, Type> NULLABLE_PRIMITIVES =
      ImmutableMap.<Schema.TypeName, Type>builder()
          .put(Schema.TypeName.BYTE, Byte.class)
          .put(Schema.TypeName.INT16, Short.class)
          .put(Schema.TypeName.INT32, Integer.class)
          .put(Schema.TypeName.INT64, Long.class)
          .put(Schema.TypeName.FLOAT, Float.class)
          .put(Schema.TypeName.DOUBLE, Double.class)
          .put(Schema.TypeName.BOOLEAN, Boolean.class)
          .put(Schema.TypeName.BYTES, byte[].class)
          .put(Schema.TypeName.STRING, String.class)
          .put(Schema.TypeName.DECIMAL, BigDecimal.class)
          .build();

  private static final Map<Schema.TypeName, Type> NON_NULLABLE_PRIMITIVES =
      ImmutableMap.<Schema.TypeName, Type>builder()
          .put(Schema.TypeName.BYTE, byte.class)
          .put(Schema.TypeName.INT16, short.class)
          .put(Schema.TypeName.INT32, int.class)
          .put(Schema.TypeName.INT64, long.class)
          .put(Schema.TypeName.FLOAT, float.class)
          .put(Schema.TypeName.DOUBLE, double.class)
          .put(Schema.TypeName.BOOLEAN, boolean.class)
          .put(Schema.TypeName.BYTES, byte[].class)
          .put(Schema.TypeName.STRING, String.class)
          .put(Schema.TypeName.DECIMAL, BigDecimal.class)
          .build();

  private static Type typeFromFieldType(Schema.FieldType fieldType) {
    Map<Schema.TypeName, Type> primitivesMap =
        fieldType.getNullable() ? NULLABLE_PRIMITIVES : NON_NULLABLE_PRIMITIVES;
    if (primitivesMap.containsKey(fieldType.getTypeName())) {
      return primitivesMap.get(fieldType.getTypeName());
    } else if (fieldType.getRowSchema() != null) {
      return Row.class;
    } else {
      throw new UnsupportedOperationException(fieldType.toString());
    }
  }
}
