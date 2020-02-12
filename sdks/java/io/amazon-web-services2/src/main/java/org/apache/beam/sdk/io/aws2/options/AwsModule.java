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
package org.apache.beam.sdk.io.aws2.options;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.type.WritableTypeId;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.auto.service.AutoService;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.http.apache.ProxyConfiguration;

/**
 * A Jackson {@link Module} that registers a {@link JsonSerializer} and {@link JsonDeserializer} for
 * {@link AwsCredentialsProvider} and some subclasses. The serialized form is a JSON map.
 */
@Experimental(Kind.SOURCE_SINK)
@AutoService(Module.class)
public class AwsModule extends SimpleModule {
  private static final String ACCESS_KEY_ID = "accessKeyId";
  private static final String SECRET_ACCESS_KEY = "secretAccessKey";

  public AwsModule() {
    super("AwsModule");
    setMixInAnnotation(AwsCredentialsProvider.class, AwsCredentialsProviderMixin.class);
    setMixInAnnotation(ProxyConfiguration.class, ProxyConfigurationMixin.class);
  }

  /** A mixin to add Jackson annotations to {@link AwsCredentialsProvider}. */
  @JsonDeserialize(using = AwsCredentialsProviderDeserializer.class)
  @JsonSerialize(using = AWSCredentialsProviderSerializer.class)
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
  private static class AwsCredentialsProviderMixin {}

  private static class AwsCredentialsProviderDeserializer
      extends JsonDeserializer<AwsCredentialsProvider> {

    @Override
    public AwsCredentialsProvider deserialize(JsonParser jsonParser, DeserializationContext context)
        throws IOException {
      return context.readValue(jsonParser, AwsCredentialsProvider.class);
    }

    @Override
    public AwsCredentialsProvider deserializeWithType(
        JsonParser jsonParser, DeserializationContext context, TypeDeserializer typeDeserializer)
        throws IOException {
      Map<String, String> asMap =
          jsonParser.readValueAs(new TypeReference<Map<String, String>>() {});

      String typeNameKey = typeDeserializer.getPropertyName();
      String typeName = asMap.get(typeNameKey);
      if (typeName == null) {
        throw new IOException(
            String.format("AWS credentials provider type name key '%s' not found", typeNameKey));
      }

      if (typeName.equals(StaticCredentialsProvider.class.getSimpleName())) {
        return StaticCredentialsProvider.create(
            AwsBasicCredentials.create(asMap.get(ACCESS_KEY_ID), asMap.get(SECRET_ACCESS_KEY)));
      } else if (typeName.equals(DefaultCredentialsProvider.class.getSimpleName())) {
        return DefaultCredentialsProvider.create();
      } else if (typeName.equals(EnvironmentVariableCredentialsProvider.class.getSimpleName())) {
        return EnvironmentVariableCredentialsProvider.create();
      } else if (typeName.equals(SystemPropertyCredentialsProvider.class.getSimpleName())) {
        return SystemPropertyCredentialsProvider.create();
      } else if (typeName.equals(ProfileCredentialsProvider.class.getSimpleName())) {
        return ProfileCredentialsProvider.create();
      } else if (typeName.equals(ContainerCredentialsProvider.class.getSimpleName())) {
        return ContainerCredentialsProvider.builder().build();
      } else {
        throw new IOException(
            String.format("AWS credential provider type '%s' is not supported", typeName));
      }
    }
  }

  private static class AWSCredentialsProviderSerializer
      extends JsonSerializer<AwsCredentialsProvider> {
    // These providers are singletons, so don't require any serialization, other than type.
    private static final ImmutableSet<Object> SINGLETON_CREDENTIAL_PROVIDERS =
        ImmutableSet.of(
            DefaultCredentialsProvider.class,
            EnvironmentVariableCredentialsProvider.class,
            SystemPropertyCredentialsProvider.class,
            ProfileCredentialsProvider.class,
            ContainerCredentialsProvider.class);

    @Override
    public void serialize(
        AwsCredentialsProvider credentialsProvider,
        JsonGenerator jsonGenerator,
        SerializerProvider serializer)
        throws IOException {
      serializer.defaultSerializeValue(credentialsProvider, jsonGenerator);
    }

    @Override
    public void serializeWithType(
        AwsCredentialsProvider credentialsProvider,
        JsonGenerator jsonGenerator,
        SerializerProvider serializer,
        TypeSerializer typeSerializer)
        throws IOException {
      WritableTypeId typeId =
          typeSerializer.writeTypePrefix(
              jsonGenerator, typeSerializer.typeId(credentialsProvider, JsonToken.START_OBJECT));
      if (credentialsProvider.getClass().equals(StaticCredentialsProvider.class)) {
        jsonGenerator.writeStringField(
            ACCESS_KEY_ID, credentialsProvider.resolveCredentials().accessKeyId());
        jsonGenerator.writeStringField(
            SECRET_ACCESS_KEY, credentialsProvider.resolveCredentials().secretAccessKey());
      } else if (!SINGLETON_CREDENTIAL_PROVIDERS.contains(credentialsProvider.getClass())) {
        throw new IllegalArgumentException(
            "Unsupported AWS credentials provider type " + credentialsProvider.getClass());
      }
      typeSerializer.writeTypeSuffix(jsonGenerator, typeId);
    }
  }

  /** A mixin to add Jackson annotations to {@link ProxyConfiguration}. */
  @JsonDeserialize(using = ProxyConfigurationDeserializer.class)
  @JsonSerialize(using = ProxyConfigurationSerializer.class)
  private static class ProxyConfigurationMixin {}

  private static class ProxyConfigurationDeserializer extends JsonDeserializer<ProxyConfiguration> {
    @Override
    public ProxyConfiguration deserialize(JsonParser jsonParser, DeserializationContext context)
        throws IOException {
      Map<String, String> asMap =
          jsonParser.readValueAs(new TypeReference<Map<String, String>>() {});
      return ProxyConfiguration.builder()
          .endpoint(URI.create(asMap.get("endpoint")))
          .username(asMap.get("username"))
          .password(asMap.get("password"))
          .useSystemPropertyValues(Boolean.valueOf(asMap.get("useSystemPropertyValues")))
          .build();
    }
  }

  private static class ProxyConfigurationSerializer extends JsonSerializer<ProxyConfiguration> {
    @Override
    public void serialize(
        ProxyConfiguration proxyConfiguration,
        JsonGenerator jsonGenerator,
        SerializerProvider serializer)
        throws IOException {
      // proxyConfiguration.endpoint() is private so we have to build it manually.
      final String endpoint =
          proxyConfiguration.scheme()
              + "://"
              + proxyConfiguration.host()
              + ":"
              + proxyConfiguration.port();
      jsonGenerator.writeStartObject();
      jsonGenerator.writeStringField("endpoint", endpoint);
      jsonGenerator.writeStringField("username", proxyConfiguration.username());
      jsonGenerator.writeStringField("password", proxyConfiguration.password());
      jsonGenerator.writeEndObject();
    }
  }
}
