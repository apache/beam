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
package org.apache.beam.sdk.expansion.service;

import static org.apache.beam.sdk.util.construction.BeamUrns.getUrn;

import com.google.auto.value.AutoValue;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.ExternalTransforms.BuilderMethod;
import org.apache.beam.model.pipeline.v1.ExternalTransforms.ExpansionMethods;
import org.apache.beam.model.pipeline.v1.ExternalTransforms.JavaClassLookupPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.ClassUtils;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.InvalidProtocolBufferException;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.yaml.snakeyaml.Yaml;

/**
 * A transform provider that can be used to directly instantiate a transform using Java class name
 * and builder methods.
 *
 * @param <InputT> input {@link PInput} type of the transform
 * @param <OutputT> output {@link POutput} type of the transform
 */
@SuppressFBWarnings("UWF_UNWRITTEN_PUBLIC_OR_PROTECTED_FIELD")
class JavaClassLookupTransformProvider<InputT extends PInput, OutputT extends POutput>
    implements TransformProvider<PInput, POutput> {

  public static final String ALLOW_LIST_VERSION = "v1";

  public static final Pattern FIELD_NAME_IGNORE_PATTERN = Pattern.compile("ignore[0-9]+");

  private static final SchemaRegistry SCHEMA_REGISTRY = SchemaRegistry.createDefault();
  private final AllowList allowList;

  public JavaClassLookupTransformProvider(AllowList allowList) {
    if (!allowList.getVersion().equals(ALLOW_LIST_VERSION)) {
      throw new IllegalArgumentException("Unknown allow-list version");
    }
    this.allowList = allowList;
  }

  @SuppressWarnings("argument")
  @Override
  public PTransform<PInput, POutput> getTransform(FunctionSpec spec, PipelineOptions options) {
    JavaClassLookupPayload payload;
    try {
      payload = JavaClassLookupPayload.parseFrom(spec.getPayload());
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException(
          "Invalid payload type for URN " + getUrn(ExpansionMethods.Enum.JAVA_CLASS_LOOKUP), e);
    }

    String className = payload.getClassName();
    try {
      AllowedClass allowlistClass = allowList.getAllowedClass(className);
      Class<PTransform<InputT, OutputT>> transformClass =
          (Class<PTransform<InputT, OutputT>>)
              ReflectHelpers.findClassLoader().loadClass(className);
      PTransform<PInput, POutput> transform;
      Row constructorRow =
          decodeRow(payload.getConstructorSchema(), payload.getConstructorPayload());
      if (payload.getConstructorMethod().isEmpty()) {
        Constructor<?>[] constructors = transformClass.getConstructors();
        Constructor<PTransform<InputT, OutputT>> constructor =
            findMappingConstructor(constructors, payload);
        Object[] parameterValues =
            getParameterValues(
                constructor.getParameters(),
                constructorRow,
                constructor.getGenericParameterTypes());
        transform = (PTransform<PInput, POutput>) constructor.newInstance(parameterValues);
      } else {
        Method[] methods = transformClass.getMethods();
        Method method = findMappingConstructorMethod(methods, payload, allowlistClass);
        Object[] parameterValues =
            getParameterValues(
                method.getParameters(), constructorRow, method.getGenericParameterTypes());
        transform = (PTransform<PInput, POutput>) method.invoke(null /* static */, parameterValues);
      }
      return applyBuilderMethods(transform, payload, allowlistClass);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Could not find class " + className, e);
    } catch (InstantiationException
        | IllegalArgumentException
        | IllegalAccessException
        | InvocationTargetException e) {
      throw new IllegalArgumentException("Could not instantiate class " + className, e);
    }
  }

  @SuppressWarnings("assignment")
  private PTransform<PInput, POutput> applyBuilderMethods(
      PTransform<PInput, POutput> transform,
      JavaClassLookupPayload payload,
      AllowedClass allowListClass) {
    for (BuilderMethod builderMethod : payload.getBuilderMethodsList()) {
      Method method = getMethod(transform, builderMethod, allowListClass);
      try {
        Row builderMethodRow = decodeRow(builderMethod.getSchema(), builderMethod.getPayload());
        transform =
            (PTransform<PInput, POutput>)
                method.invoke(
                    transform,
                    getParameterValues(
                        method.getParameters(),
                        builderMethodRow,
                        method.getGenericParameterTypes()));
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new IllegalArgumentException(
            "Could not invoke the builder method "
                + builderMethod
                + " on transform "
                + transform
                + " with parameter schema "
                + builderMethod.getSchema(),
            e);
      }
    }

    return transform;
  }

  private boolean isBuilderMethodForName(
      Method method, String nameFromPayload, AllowedClass allowListClass) {
    // Lookup based on method annotations
    for (Annotation annotation : method.getAnnotations()) {
      if (annotation instanceof MultiLanguageBuilderMethod) {
        if (nameFromPayload.equals(((MultiLanguageBuilderMethod) annotation).name())) {
          if (allowListClass.isAllowedBuilderMethod(nameFromPayload)) {
            return true;
          } else {
            throw new RuntimeException(
                "Builder method " + nameFromPayload + " has to be explicitly allowed");
          }
        }
      }
    }

    // Lookup based on the method name.
    boolean match = method.getName().equals(nameFromPayload);
    String consideredMethodName = method.getName();

    // We provide a simplification for common Java builder pattern naming convention where builder
    // methods start with "with". In this case, for a builder method name in the form "withXyz",
    // users may just use "xyz". If additional updates to the method name are needed the transform
    // has to be updated by adding annotations.
    if (!match && consideredMethodName.length() > 4 && consideredMethodName.startsWith("with")) {
      consideredMethodName =
          consideredMethodName.substring(4, 5).toLowerCase() + consideredMethodName.substring(5);
      match = consideredMethodName.equals(nameFromPayload);
    }
    if (match && !allowListClass.isAllowedBuilderMethod(consideredMethodName)) {
      throw new RuntimeException(
          "Builder method name " + consideredMethodName + " has to be explicitly allowed");
    }
    return match;
  }

  private Method getMethod(
      PTransform<PInput, POutput> transform,
      BuilderMethod builderMethod,
      AllowedClass allowListClass) {

    Row builderMethodRow = decodeRow(builderMethod.getSchema(), builderMethod.getPayload());

    List<Method> matchingMethods =
        Arrays.stream(transform.getClass().getMethods())
            .filter(m -> isBuilderMethodForName(m, builderMethod.getName(), allowListClass))
            .filter(m -> parametersCompatible(m.getParameters(), builderMethodRow))
            .filter(m -> PTransform.class.isAssignableFrom(m.getReturnType()))
            .collect(Collectors.toList());

    if (matchingMethods.size() == 0) {
      throw new RuntimeException(
          "Could not find a matching method in transform "
              + transform
              + " for BuilderMethod"
              + builderMethod
              + ". When using field names, make sure they are available in the compiled"
              + " Java class.");
    } else if (matchingMethods.size() > 1) {
      throw new RuntimeException(
          "Expected to find exactly one matching method in transform "
              + transform
              + " for BuilderMethod"
              + builderMethod
              + " but found "
              + matchingMethods.size());
    }
    return matchingMethods.get(0);
  }

  private static boolean isPrimitiveOrWrapperOrString(java.lang.Class<?> type) {
    return ClassUtils.isPrimitiveOrWrapper(type) || type == String.class;
  }

  private Schema getParameterSchema(Class<?> parameterClass) {
    Schema parameterSchema;
    try {
      parameterSchema = SCHEMA_REGISTRY.getSchema(parameterClass);
    } catch (NoSuchSchemaException e) {

      SCHEMA_REGISTRY.registerSchemaProvider(parameterClass, new JavaFieldSchema());
      try {
        parameterSchema = SCHEMA_REGISTRY.getSchema(parameterClass);
      } catch (NoSuchSchemaException e1) {
        throw new RuntimeException(e1);
      }
      if (parameterSchema != null && parameterSchema.getFieldCount() == 0) {
        throw new RuntimeException(
            "Could not determine a valid schema for parameter class " + parameterClass);
      }
    }
    return parameterSchema;
  }

  private boolean parametersCompatible(
      java.lang.reflect.Parameter[] methodParameters, Row constructorRow) {
    Schema constructorSchema = constructorRow.getSchema();
    if (methodParameters.length != constructorSchema.getFieldCount()) {
      return false;
    }

    for (int i = 0; i < methodParameters.length; i++) {
      java.lang.reflect.Parameter parameterFromReflection = methodParameters[i];
      Field parameterFromPayload = constructorSchema.getField(i);
      String paramNameFromReflection = parameterFromReflection.getName();

      // Spec requires field names in this format to be ignored.
      boolean ignoreFieldName =
          FIELD_NAME_IGNORE_PATTERN.matcher(parameterFromPayload.getName()).matches();

      if (!ignoreFieldName && !paramNameFromReflection.equals(parameterFromPayload.getName())) {
        // Parameter name through reflection is from the class file (not through synthesizing,
        // hence we can validate names)
        return false;
      }

      Class<?> parameterClass = parameterFromReflection.getType();
      if (isPrimitiveOrWrapperOrString(parameterClass)) {
        continue;
      }

      // We perform additional validation for arrays and non-primitive types.
      if (parameterClass.isArray()) {
        Class<?> arrayFieldClass = parameterClass.getComponentType();
        if (parameterFromPayload.getType().getTypeName() != TypeName.ARRAY) {
          throw new RuntimeException(
              "Expected a schema with a single array field but received "
                  + parameterFromPayload.getType().getTypeName());
        }

        // Following is a best-effort validation that may not cover all cases. Idea is to resolve
        // ambiguities as much as possible to determine an exact match for the given set of
        // parameters. If there are ambiguities, the expansion will fail.
        if (!isPrimitiveOrWrapperOrString(arrayFieldClass)) {
          @Nullable Collection<Row> values = constructorRow.getArray(i);
          Schema arrayFieldSchema = getParameterSchema(arrayFieldClass);
          if (arrayFieldSchema == null) {
            throw new RuntimeException("Could not determine a schema for type " + arrayFieldClass);
          }
          if (values != null) {
            @Nullable Row firstItem = values.iterator().next();
            if (firstItem != null && !firstItem.getSchema().assignableTo(arrayFieldSchema)) {
              return false;
            }
          }
        }
      } else if (constructorRow.getValue(i) instanceof Row) {
        @Nullable Row parameterRow = constructorRow.getRow(i);
        Schema schema = getParameterSchema(parameterClass);
        if (schema == null) {
          throw new RuntimeException("Could not determine a schema for type " + parameterClass);
        }
        if (parameterRow != null && !parameterRow.getSchema().assignableTo(schema)) {
          return false;
        }
      }
    }
    return true;
  }

  @SuppressWarnings("argument")
  private @Nullable Object getDecodedValueFromRow(
      Class<?> type, Object valueFromRow, @Nullable Type genericType) {
    if (isPrimitiveOrWrapperOrString(type)) {
      if (!isPrimitiveOrWrapperOrString(valueFromRow.getClass())) {
        throw new IllegalArgumentException(
            "Expected a Java primitive value but received " + valueFromRow);
      }
      return valueFromRow;
    } else if (type.isArray()) {
      Class<?> arrayComponentClass = type.getComponentType();
      return getDecodedArrayValueFromRow(arrayComponentClass, valueFromRow);
    } else if (Collection.class.isAssignableFrom(type)) {
      List<Object> originalList = (List) valueFromRow;
      List<Object> decodedList = new ArrayList<>();
      for (Object obj : originalList) {
        if (genericType instanceof ParameterizedType) {
          Class<?> elementType =
              (Class<?>) ((ParameterizedType) genericType).getActualTypeArguments()[0];
          decodedList.add(getDecodedValueFromRow(elementType, obj, null));
        } else {
          throw new RuntimeException("Could not determine the generic type of the list");
        }
      }
      return decodedList;
    } else if (valueFromRow instanceof Row) {
      Row row = (Row) valueFromRow;
      SerializableFunction<Row, ?> fromRowFunc;
      try {
        fromRowFunc = SCHEMA_REGISTRY.getFromRowFunction(type);
      } catch (NoSuchSchemaException e) {
        throw new IllegalArgumentException(
            "Could not determine the row function for class " + type, e);
      }
      return fromRowFunc.apply(row);
    }
    throw new RuntimeException("Could not decode the value from Row " + valueFromRow);
  }

  @SuppressWarnings("argument")
  private Object[] getParameterValues(
      java.lang.reflect.Parameter[] parameters, Row constrtuctorRow, Type[] genericTypes) {
    ArrayList<Object> parameterValues = new ArrayList<>();
    for (int i = 0; i < parameters.length; ++i) {
      java.lang.reflect.Parameter parameter = parameters[i];
      Class<?> parameterClass = parameter.getType();
      Object parameterValue =
          getDecodedValueFromRow(parameterClass, constrtuctorRow.getValue(i), genericTypes[i]);
      parameterValues.add(parameterValue);
    }

    return parameterValues.toArray();
  }

  @SuppressWarnings("argument")
  private Object[] getDecodedArrayValueFromRow(Class<?> arrayComponentType, Object valueFromRow) {
    List<Object> originalValues = (List<Object>) valueFromRow;
    List<Object> decodedValues = new ArrayList<>();
    for (Object obj : originalValues) {
      decodedValues.add(getDecodedValueFromRow(arrayComponentType, obj, null));
    }

    // We have to construct and return an array of the correct type. Otherwise Java reflection
    // constructor/method invocations that use the returned value may consider the array as varargs
    // (different parameters).
    Object valueTypeArray = Array.newInstance(arrayComponentType, decodedValues.size());
    for (int i = 0; i < decodedValues.size(); i++) {
      Array.set(valueTypeArray, i, arrayComponentType.cast(decodedValues.get(i)));
    }
    return (Object[]) valueTypeArray;
  }

  private Constructor<PTransform<InputT, OutputT>> findMappingConstructor(
      Constructor<?>[] constructors, JavaClassLookupPayload payload) {
    Row constructorRow = decodeRow(payload.getConstructorSchema(), payload.getConstructorPayload());

    List<Constructor<?>> mappingConstructors =
        Arrays.stream(constructors)
            .filter(c -> c.getParameterCount() == payload.getConstructorSchema().getFieldsCount())
            .filter(c -> parametersCompatible(c.getParameters(), constructorRow))
            .collect(Collectors.toList());

    if (mappingConstructors.size() == 0) {
      throw new RuntimeException(
          "Could not find a matching constructor. When using field names, make sure they are "
              + "available in the compiled Java class.");
    } else if (mappingConstructors.size() != 1) {
      throw new RuntimeException(
          "Expected to find a single mapping constructor but found " + mappingConstructors.size());
    }
    return (Constructor<PTransform<InputT, OutputT>>) mappingConstructors.get(0);
  }

  private boolean isConstructorMethodForName(
      Method method, String nameFromPayload, AllowedClass allowListClass) {
    for (Annotation annotation : method.getAnnotations()) {
      if (annotation instanceof MultiLanguageConstructorMethod) {
        if (nameFromPayload.equals(((MultiLanguageConstructorMethod) annotation).name())) {
          if (allowListClass.isAllowedConstructorMethod(nameFromPayload)) {
            return true;
          } else {
            throw new RuntimeException(
                "Constructor method " + nameFromPayload + " needs to be explicitly allowed");
          }
        }
      }
    }
    if (method.getName().equals(nameFromPayload)) {
      if (allowListClass.isAllowedConstructorMethod(nameFromPayload)) {
        return true;
      } else {
        throw new RuntimeException(
            "Constructor method " + nameFromPayload + " needs to be explicitly allowed");
      }
    }
    return false;
  }

  private Method findMappingConstructorMethod(
      Method[] methods, JavaClassLookupPayload payload, AllowedClass allowListClass) {

    Row constructorRow = decodeRow(payload.getConstructorSchema(), payload.getConstructorPayload());

    List<Method> mappingConstructorMethods =
        Arrays.stream(methods)
            .filter(
                m -> isConstructorMethodForName(m, payload.getConstructorMethod(), allowListClass))
            .filter(m -> m.getParameterCount() == payload.getConstructorSchema().getFieldsCount())
            .filter(m -> parametersCompatible(m.getParameters(), constructorRow))
            .collect(Collectors.toList());

    if (mappingConstructorMethods.size() == 0) {
      throw new RuntimeException(
          "Could not find a matching constructor method. When using field names, make sure they "
              + "are available in the compiled Java class.");
    } else if (mappingConstructorMethods.size() != 1) {
      throw new RuntimeException(
          "Expected to find a single mapping constructor method but found "
              + mappingConstructorMethods.size()
              + " Payload was "
              + payload);
    }
    return mappingConstructorMethods.get(0);
  }

  @AutoValue
  public abstract static class AllowList {

    public static AllowList nothing() {
      return create(ALLOW_LIST_VERSION, Collections.emptyList());
    }

    public static AllowList everything() {
      return create(
          ALLOW_LIST_VERSION,
          Collections.singletonList(
              AllowedClass.create("*", AllowedClass.WILDCARD, AllowedClass.WILDCARD)));
    }

    static AllowList parseFromYamlStream(InputStream inputStream) {
      Yaml yaml = new Yaml();
      Map<Object, Object> config = yaml.load(inputStream);

      if (config == null) {
        throw new IllegalArgumentException(
            "Could not parse the provided YAML stream into a non-trivial AllowList");
      }

      String version = config.get("version") != null ? (String) config.get("version") : "";
      List<AllowedClass> allowedClasses = new ArrayList<>();
      if (config.get("allowedClasses") != null) {
        allowedClasses =
            ((List<Map<Object, Object>>) config.get("allowedClasses"))
                .stream()
                    .map(
                        data -> {
                          String className = (String) data.get("className");
                          if (className == null) {
                            throw new IllegalArgumentException(
                                "Expected each entry in the allowlist to include the 'className'");
                          }
                          List<String> allowedBuilderMethods =
                              (List<String>) data.get("allowedBuilderMethods");
                          List<String> allowedConstructorMethods =
                              (List<String>) data.get("allowedConstructorMethods");
                          if (allowedBuilderMethods == null) {
                            allowedBuilderMethods = new ArrayList<>();
                          }
                          if (allowedConstructorMethods == null) {
                            allowedConstructorMethods = new ArrayList<>();
                          }
                          return AllowedClass.create(
                              className, allowedBuilderMethods, allowedConstructorMethods);
                        })
                    .collect(Collectors.toList());
      }
      return AllowList.create(version, allowedClasses);
    }

    public abstract String getVersion();

    public abstract List<AllowedClass> getAllowedClasses();

    public AllowedClass getAllowedClass(String className) {
      AllowedClass allowlistClass = null;
      for (AllowedClass cls : getAllowedClasses()) {
        if (cls.isAllowedClass(className)) {
          if (allowlistClass != null) {
            throw new IllegalArgumentException(
                "Found two matching allowlist classes " + allowlistClass + " and " + cls);
          }
          allowlistClass = cls;
        }
      }
      if (allowlistClass == null) {
        throw new UnsupportedOperationException(
            "The provided allow list does not enable expanding a transform class by the name "
                + className
                + ".");
      }
      return allowlistClass;
    }

    static AllowList create(String version, List<AllowedClass> allowedClasses) {
      if (allowedClasses == null) {
        allowedClasses = new ArrayList<>();
      }
      return new AutoValue_JavaClassLookupTransformProvider_AllowList(version, allowedClasses);
    }
  }

  @AutoValue
  public abstract static class AllowedClass {

    public static final List<String> WILDCARD = Collections.singletonList("*");

    public abstract String getClassName();

    public abstract List<String> getAllowedBuilderMethods();

    public abstract List<String> getAllowedConstructorMethods();

    public boolean isAllowedClass(String className) {
      String pattern = getClassName();
      return pattern.equals(className)
          || pattern.equals("*")
          || (pattern.endsWith(".*")
              && className.startsWith(pattern.substring(0, pattern.length() - 2)));
    }

    public boolean isAllowedBuilderMethod(String methodName) {
      return getAllowedBuilderMethods().contains(methodName)
          || getAllowedBuilderMethods().equals(WILDCARD);
    }

    public boolean isAllowedConstructorMethod(String methodName) {
      return getAllowedConstructorMethods().contains(methodName)
          || getAllowedConstructorMethods().equals(WILDCARD);
    }

    static AllowedClass create(
        String className,
        List<String> allowedBuilderMethods,
        List<String> allowedConstructorMethods) {
      if (allowedBuilderMethods == null) {
        allowedBuilderMethods = new ArrayList<>();
      }
      if (allowedConstructorMethods == null) {
        allowedConstructorMethods = new ArrayList<>();
      }
      if (allowedBuilderMethods.equals(WILDCARD) && !className.equals("*")) {
        // If we allow getClass().forName(), we allow essentially anything.
        throw new IllegalArgumentException("Wildcard builder not allowed for non-wildcard class.");
      }
      return new AutoValue_JavaClassLookupTransformProvider_AllowedClass(
          className, allowedBuilderMethods, allowedConstructorMethods);
    }
  }

  static Row decodeRow(SchemaApi.Schema schema, ByteString payload) {
    Schema payloadSchema = SchemaTranslation.schemaFromProto(schema);

    if (payloadSchema.getFieldCount() == 0) {
      return Row.withSchema(Schema.of()).build();
    }

    Row row;
    try {
      row = RowCoder.of(payloadSchema).decode(payload.newInput());
    } catch (IOException e) {
      throw new RuntimeException("Error decoding payload", e);
    }
    return row;
  }
}
