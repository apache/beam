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

import static org.apache.beam.runners.core.construction.BeamUrns.getUrn;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.ExternalTransforms.BuilderMethod;
import org.apache.beam.model.pipeline.v1.ExternalTransforms.ExpansionMethods;
import org.apache.beam.model.pipeline.v1.ExternalTransforms.JavaClassLookupPayload;
import org.apache.beam.model.pipeline.v1.ExternalTransforms.Parameter;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.ClassUtils;
import org.apache.beam.sdk.expansion.service.ExpansionService.ExternalTransformRegistrarLoader;
import org.apache.beam.sdk.expansion.service.ExpansionService.TransformProvider;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p36p0.com.google.protobuf.InvalidProtocolBufferException;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * A transform provider that can be used to directly instantiate a transform using Java class name
 * and builder methods.
 *
 * @param <InputT> input {@link PInput} type of the transform
 * @param <OutputT> output {@link POutput} type of the transform
 */
@SuppressWarnings({"argument.type.incompatible", "assignment.type.incompatible"})
@SuppressFBWarnings("UWF_UNWRITTEN_PUBLIC_OR_PROTECTED_FIELD")
class JavaClassLookupTransformProvider<InputT extends PInput, OutputT extends POutput>
    implements TransformProvider<PInput, POutput> {

  public static final String ALLOW_LIST_VERSION = "v1";
  private static final SchemaRegistry SCHEMA_REGISTRY = SchemaRegistry.createDefault();
  private final AllowList allowList;

  public JavaClassLookupTransformProvider(AllowList allowList) {
    if (!allowList.getVersion().equals(ALLOW_LIST_VERSION)) {
      throw new IllegalArgumentException("Unknown allow-list version");
    }
    this.allowList = allowList;
  }

  @Override
  public PTransform<PInput, POutput> getTransform(FunctionSpec spec) {
    JavaClassLookupPayload payload = null;
    try {
      payload = JavaClassLookupPayload.parseFrom(spec.getPayload());
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException(
          "Invalid payload type for URN " + getUrn(ExpansionMethods.Enum.JAVA_CLASS_LOOKUP), e);
    }

    String className = payload.getClassName();
    try {
      AllowedClass allowlistClass = null;
      if (this.allowList != null) {
        for (AllowedClass cls : this.allowList.getAllowedClasses()) {
          if (cls.getClassName().equals(className)) {
            if (allowlistClass != null) {
              throw new IllegalArgumentException(
                  "Found two matching allowlist classes " + allowlistClass + " and " + cls);
            }
            allowlistClass = cls;
          }
        }
      }
      if (allowlistClass == null) {
        throw new UnsupportedOperationException(
            "The provided allow list does not enable expanding a transform class by the name " + className + ".");
      }
      Class<PTransform<InputT, OutputT>> transformClass =
          (Class<PTransform<InputT, OutputT>>)
              ReflectHelpers.findClassLoader().loadClass(className);
      PTransform<PInput, POutput> transform;
      if (payload.getConstructorMethod().isEmpty()) {
        Constructor<?>[] constructors = transformClass.getConstructors();
        Constructor<PTransform<InputT, OutputT>> constructor =
            findMappingConstructor(constructors, payload);
        Object[] parameterValues =
            getParameterValues(
                constructor.getParameters(),
                payload.getConstructorParametersList().toArray(new Parameter[0]));
        transform = (PTransform<PInput, POutput>) constructor.newInstance(parameterValues);
      } else {
        Method[] methods = transformClass.getMethods();
        Method method = findMappingConstructorMethod(methods, payload, allowlistClass);
        Object[] parameterValues =
            getParameterValues(
                method.getParameters(),
                payload.getConstructorParametersList().toArray(new Parameter[0]));
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

  private PTransform<PInput, POutput> applyBuilderMethods(
      PTransform<PInput, POutput> transform,
      JavaClassLookupPayload payload,
      AllowedClass allowListClass) {
    for (BuilderMethod builderMethod : payload.getBuilderMethodsList()) {
      Method method = getMethod(transform, builderMethod, allowListClass);
      try {
        transform =
            (PTransform<PInput, POutput>)
                method.invoke(
                    transform,
                    getParameterValues(
                        method.getParameters(),
                        builderMethod.getParameterList().toArray(new Parameter[0])));
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new IllegalArgumentException(
            "Could not invoke the builder method "
                + builderMethod
                + " on transform "
                + transform
                + " with parameters "
                + builderMethod.getParameterList(),
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
          if (allowListClass.getAllowedBuilderMethods().contains(nameFromPayload)) {
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
    if (match && !allowListClass.getAllowedBuilderMethods().contains(consideredMethodName)) {
      throw new RuntimeException(
          "Builder method name " + consideredMethodName + " has to be explicitly allowed");
    }
    return match;
  }

  private Method getMethod(
      PTransform<PInput, POutput> transform,
      BuilderMethod builderMethod,
      AllowedClass allowListClass) {
    List<Method> matchingMethods =
        Arrays.stream(transform.getClass().getMethods())
            .filter(m -> isBuilderMethodForName(m, builderMethod.getName(), allowListClass))
            .filter(
                m ->
                    parametersCompatible(
                        m.getParameters(),
                        builderMethod.getParameterList().toArray(new Parameter[0])))
            .filter(m -> PTransform.class.isAssignableFrom(m.getReturnType()))
            .collect(Collectors.toList());

    if (matchingMethods.size() != 1) {
      throw new RuntimeException(
          "Expected to find exact one matching method in transform "
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

  private boolean parametersCompatible(
      java.lang.reflect.Parameter[] methodParameters, Parameter[] payloadParameters) {
    if (methodParameters.length != payloadParameters.length) {
      return false;
    }

    for (int i = 0; i < methodParameters.length; i++) {
      java.lang.reflect.Parameter parameterFromReflection = methodParameters[i];
      Parameter parameterFromPayload = payloadParameters[i];

      String paramNameFromReflection = parameterFromReflection.getName();
      if (!paramNameFromReflection.startsWith("arg")
          && !paramNameFromReflection.equals(parameterFromPayload.getName())) {
        // Parameter name through reflection is from the class file (not through synthesizing,
        // hence we can validate names)
        return false;
      }

      Class<PTransform<InputT, OutputT>> parameterClass =
          (Class<PTransform<InputT, OutputT>>) parameterFromReflection.getType();
      Row parameterRow =
          ExternalTransformRegistrarLoader.decodeRow(
              parameterFromPayload.getSchema(), parameterFromPayload.getPayload());

      Schema parameterSchema = null;
      if (isPrimitiveOrWrapperOrString(parameterClass)) {
        if (parameterRow.getFieldCount() != 1) {
          throw new RuntimeException(
              "Expected a row for a single primitive field but received " + parameterRow);
        }
        // We get the value just for validation here.
        getPrimitiveValueFromRow(parameterRow);
      } else {
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
      }

      if (parameterSchema != null && !parameterRow.getSchema().assignableTo(parameterSchema)) {
        return false;
      }
    }
    return true;
  }

  private Object[] getParameterValues(
      java.lang.reflect.Parameter[] parameters, Parameter[] payloadParameters) {
    ArrayList<Object> parameterValues = new ArrayList<>();
    for (int i = 0; i < parameters.length; ++i) {
      Parameter parameter = parameters[i];
      Parameter parameterConfig = payloadParameters[i];
      Class<?> parameterClass = parameter.getType();

      Row parameterRow =
          ExternalTransformRegistrarLoader.decodeRow(
              parameterConfig.getSchema(), parameterConfig.getPayload());

      Object parameterValue = null;
      if (isPrimitiveOrWrapperOrString(parameterClass)) {
        parameterValue = getPrimitiveValueFromRow(parameterRow);
      } else {
        SerializableFunction<Row, ?> fromRowFunc = null;
        // SCHEMA_REGISTRY.
        try {
          fromRowFunc = SCHEMA_REGISTRY.getFromRowFunction(parameterClass);
        } catch (NoSuchSchemaException e) {
          throw new IllegalArgumentException(
              "Could not determine the row function for class " + parameterClass, e);
        }
        parameterValue = fromRowFunc.apply(parameterRow);
      }
      parameterValues.add(parameterValue);
    }

    return parameterValues.toArray();
  }

  private Object getPrimitiveValueFromRow(Row parameterRow) {
    if (parameterRow.getFieldCount() != 1) {
      throw new IllegalArgumentException(
          "Expected a Row that contains a single field but received " + parameterRow);
    }

    @NonNull Object value = parameterRow.getValue(0);
    if (!isPrimitiveOrWrapperOrString(value.getClass())) {
      throw new IllegalArgumentException("Expected a Java primitive value but received " + value);
    }
    return value;
  }

  private Constructor<PTransform<InputT, OutputT>> findMappingConstructor(
      Constructor<?>[] constructors, JavaClassLookupPayload payload) {
    Parameter[] constructorParametersFromPayload =
        payload.getConstructorParametersList().toArray(new Parameter[0]);
    List<Constructor<?>> mappingConstructors =
        Arrays.stream(constructors)
            .filter(c -> c.getParameterCount() == payload.getConstructorParametersCount())
            .filter(c -> parametersCompatible(c.getParameters(), constructorParametersFromPayload))
            .collect(Collectors.toList());
    if (mappingConstructors.size() != 1) {
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
          if (allowListClass.getAllowedConstructorMethods().contains(nameFromPayload)) {
            return true;
          } else {
            throw new RuntimeException(
                "Constructor method " + nameFromPayload + " needs to be explicitly allowed");
          }
        }
      }
    }
    if (method.getName().equals(nameFromPayload)) {
      if (allowListClass.getAllowedConstructorMethods().contains(nameFromPayload)) {
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
    Parameter[] constructporMethodParametersFromPayload =
        payload.getConstructorParametersList().toArray(new Parameter[0]);
    List<Method> mappingConstructorMethods =
        Arrays.stream(methods)
            .filter(
                m -> isConstructorMethodForName(m, payload.getConstructorMethod(), allowListClass))
            .filter(m -> m.getParameterCount() == payload.getConstructorParametersCount())
            .filter(
                m ->
                    parametersCompatible(
                        m.getParameters(), constructporMethodParametersFromPayload))
            .collect(Collectors.toList());

    if (mappingConstructorMethods.size() != 1) {
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

    public abstract String getVersion();

    public abstract List<AllowedClass> getAllowedClasses();

    @JsonCreator
    static AllowList create(
        @JsonProperty("version") String version,
        @JsonProperty("allowedClasses") @javax.annotation.Nullable
            List<AllowedClass> allowedClasses) {
      if (allowedClasses == null) {
        allowedClasses = new ArrayList<>();
      }
      return new AutoValue_JavaClassLookupTransformProvider_AllowList(version, allowedClasses);
    }
  }

  @AutoValue
  public abstract static class AllowedClass {

    public abstract String getClassName();

    public abstract List<String> getAllowedBuilderMethods();

    public abstract List<String> getAllowedConstructorMethods();

    @JsonCreator
    static AllowedClass create(
        @JsonProperty("className") String className,
        @JsonProperty("allowedBuilderMethods") @javax.annotation.Nullable
            List<String> allowedBuilderMethods,
        @JsonProperty("allowedConstructorMethods") @javax.annotation.Nullable
            List<String> allowedConstructorMethods) {
      if (allowedBuilderMethods == null) {
        allowedBuilderMethods = new ArrayList<>();
      }
      if (allowedConstructorMethods == null) {
        allowedConstructorMethods = new ArrayList<>();
      }
      return new AutoValue_JavaClassLookupTransformProvider_AllowedClass(
          className, allowedBuilderMethods, allowedConstructorMethods);
    }
  }
}
