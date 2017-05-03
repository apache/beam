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

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.util.PropertyNames;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * Utility for converting objects between Java and Cloud representations.
 *
 * @deprecated Will no longer be used once all coders are converted via {@link CloudObjects}.
 */
@Deprecated
public final class Serializer {
  /** A mapping from well known coder types to their implementing classes. */
  private static final Map<String, Class<?>> WELL_KNOWN_CODER_TYPES =
      ImmutableMap.<String, Class<?>>builder()
          .put("kind:pair", KvCoder.class)
          .put("kind:stream", IterableCoder.class)
          .put("kind:global_window", GlobalWindow.Coder.class)
          .put("kind:interval_window", IntervalWindow.IntervalWindowCoder.class)
          .put("kind:length_prefix", LengthPrefixCoder.class)
          .put("kind:windowed_value", WindowedValue.FullWindowedValueCoder.class)
          .build();

  // Delay initialization of statics until the first call to Serializer.
  private static class SingletonHelper {
    static final ObjectMapper OBJECT_MAPPER = createObjectMapper();
    static final ObjectMapper TREE_MAPPER = createTreeMapper();

    /**
     * Creates the object mapper that will be used for serializing Google API
     * client maps into Jackson trees.
     */
    private static ObjectMapper createTreeMapper() {
      return new ObjectMapper();
    }

    /**
     * Creates the object mapper that will be used for deserializing Jackson
     * trees into objects.
     */
    private static ObjectMapper createObjectMapper() {
      ObjectMapper m = new ObjectMapper();
      // Ignore properties that are not used by the object.
      m.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

      // For parameters of type Object, use the @type property to determine the
      // class to instantiate.
      //
      // TODO: It would be ideal to do this for all non-final classes.  The
      // problem with using DefaultTyping.NON_FINAL is that it insists on having
      // type information in the JSON for classes with useful default
      // implementations, such as List.  Ideally, we'd combine these defaults
      // with available type information if that information's present.
      m.enableDefaultTypingAsProperty(
           ObjectMapper.DefaultTyping.JAVA_LANG_OBJECT,
           PropertyNames.OBJECT_TYPE_NAME);

      m.registerModule(new Jackson2Module());

      return m;
    }
  }

  /**
   * Deserializes an object from a Dataflow structured encoding (represented in
   * Java as a map).
   *
   * <p>The standard Dataflow SDK object serialization protocol is based on JSON.
   * Data is typically encoded as a JSON object whose fields represent the
   * object's data.
   *
   * <p>The actual deserialization is performed by Jackson, which can deserialize
   * public fields, use JavaBean setters, or use injection annotations to
   * indicate how to construct the object.  The {@link ObjectMapper} used is
   * configured to use the "@type" field as the name of the class to instantiate
   * (supporting polymorphic types), and may be further configured by
   * annotations or via {@link ObjectMapper#registerModule}.
   *
   * @see <a href="http://wiki.fasterxml.com/JacksonFAQ#Data_Binding.2C_general">
   * Jackson Data-Binding</a>
   * @see <a href="https://github.com/FasterXML/jackson-annotations/wiki/Jackson-Annotations">
   * Jackson-Annotations</a>
   * @param serialized the object in untyped decoded form (i.e. a nested {@link Map})
   * @param clazz the expected object class
   */
  public static <T> T deserialize(Map<String, Object> serialized, Class<T> clazz) {
    try {
      return SingletonHelper.OBJECT_MAPPER.treeToValue(
          SingletonHelper.TREE_MAPPER.valueToTree(
              deserializeCloudKnownTypes(serialized)),
          clazz);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(
          "Unable to deserialize class " + clazz, e);
    }
  }

  /**
   * Recursively walks the supplied map, looking for well-known cloud type information (keyed as
   * {@link PropertyNames#OBJECT_TYPE_NAME}, matching a URI value from the {@link CloudKnownType}
   * enum. Upon finding this type information, it converts it into the correspondingly typed Java
   * value.
   */
  @SuppressWarnings("unchecked")
  private static Object deserializeCloudKnownTypes(Object src) {
    if (src instanceof Map) {
      Map<String, Object> srcMap = (Map<String, Object>) src;
      @Nullable Object value = srcMap.get(PropertyNames.SCALAR_FIELD_NAME);
      @Nullable CloudKnownType type =
          CloudKnownType.forUri((String) srcMap.get(PropertyNames.OBJECT_TYPE_NAME));
      if (type != null && value != null) {
        // It's a value of a well-known cloud type; let the known type handler
        // handle the translation.
        Object result = type.parse(value, type.defaultClass());
        return result;
      }
      // Otherwise, it's just an ordinary map.
      Map<String, Object> dest = new HashMap<>(srcMap.size());
      for (Map.Entry<String, Object> entry : srcMap.entrySet()) {
        dest.put(entry.getKey(), deserializeCloudKnownTypes(entry.getValue()));
      }
      return dest;
    }
    if (src instanceof List) {
      List<Object> srcList = (List<Object>) src;
      List<Object> dest = new ArrayList<>(srcList.size());
      for (Object obj : srcList) {
        dest.add(deserializeCloudKnownTypes(obj));
      }
      return dest;
    }
    // Neither a Map nor a List; no translation needed.
    return src;
  }

  /**
   * A {@link com.fasterxml.jackson.databind.Module} that adds the type
   * resolver needed for Coder definitions.
   */
  static final class Jackson2Module extends SimpleModule {
    /**
     * The Coder custom type resolver.
     *
     * <p>This resolver resolves coders. If the Coder ID is a particular
     * well-known identifier, it's replaced with the corresponding class.
     * All other Coder instances are resolved by class name, using the package
     * org.apache.beam.sdk.coders if there are no "."s in the ID.
     */
    private static final class Resolver extends TypeIdResolverBase {
      @SuppressWarnings("unused") // Used via @JsonTypeIdResolver annotation on Mixin
      public Resolver() {
        super(TypeFactory.defaultInstance().constructType(Coder.class),
            TypeFactory.defaultInstance());
      }

      @Override
      public JavaType typeFromId(DatabindContext context, String id) {
        Class<?> clazz = getClassForId(id);
        @SuppressWarnings("rawtypes")
        TypeVariable[] tvs = clazz.getTypeParameters();
        JavaType[] types = new JavaType[tvs.length];
        for (int lupe = 0; lupe < tvs.length; lupe++) {
          types[lupe] = TypeFactory.unknownType();
        }
        return _typeFactory.constructSimpleType(clazz, types);
      }

      private Class<?> getClassForId(String id) {
        try {
          if (id.contains(".")) {
            return Class.forName(id);
          }

          if (WELL_KNOWN_CODER_TYPES.containsKey(id)) {
            return WELL_KNOWN_CODER_TYPES.get(id);
          }

          // Otherwise, see if the ID is the name of a class in
          // org.apache.beam.sdk.coders.  We do this via creating
          // the class object so that class loaders have a chance to get
          // involved -- and since we need the class object anyway.
          return Class.forName(Coder.class.getPackage().getName() + "." + id);
        } catch (ClassNotFoundException e) {
          throw new RuntimeException("Unable to convert coder ID " + id + " to class", e);
        }
      }

      @Override
      public String idFromValueAndType(Object o, Class<?> clazz) {
        return clazz.getName();
      }

      @Override
      public String idFromValue(Object o) {
        return o.getClass().getName();
      }

      @Override
      public JsonTypeInfo.Id getMechanism() {
        return JsonTypeInfo.Id.CUSTOM;
      }
    }

    /**
     * The mixin class defining how Coders are handled by the deserialization
     * {@link ObjectMapper}.
     *
     * <p>This is done via a mixin so that this resolver is <i>only</i> used
     * during deserialization requested by the Apache Beam SDK.
     */
    @JsonTypeIdResolver(Resolver.class)
    @JsonTypeInfo(use = Id.CUSTOM, include = As.PROPERTY, property = PropertyNames.OBJECT_TYPE_NAME)
    private static final class Mixin {}

    public Jackson2Module() {
      super("BeamCoders");
      setMixInAnnotation(Coder.class, Mixin.class);
    }
  }
}
