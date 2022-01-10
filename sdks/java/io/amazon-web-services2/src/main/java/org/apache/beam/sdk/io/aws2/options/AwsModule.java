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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.NameTransformer;
import com.google.auto.service.AutoService;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.io.aws2.s3.SSECustomerKey;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.utils.AttributeMap;

/**
 * A Jackson {@link Module} that registers a {@link JsonSerializer} and {@link JsonDeserializer} for
 * {@link AwsCredentialsProvider} and some subclasses. The serialized form is a JSON map.
 */
@Experimental(Kind.SOURCE_SINK)
@AutoService(Module.class)
public class AwsModule extends SimpleModule {
  private static final String ACCESS_KEY_ID = "accessKeyId";
  private static final String SECRET_ACCESS_KEY = "secretAccessKey";
  private static final String SESSION_TOKEN = "sessionToken";
  public static final String CONNECTION_ACQUIRE_TIMEOUT = "connectionAcquisitionTimeout";
  public static final String CONNECTION_MAX_IDLE_TIMEOUT = "connectionMaxIdleTime";
  public static final String CONNECTION_TIMEOUT = "connectionTimeout";
  public static final String CONNECTION_TIME_TO_LIVE = "connectionTimeToLive";
  public static final String MAX_CONNECTIONS = "maxConnections";
  public static final String READ_TIMEOUT = "socketTimeout";

  @SuppressWarnings({"nullness"})
  public AwsModule() {
    super("AwsModule");
    setMixInAnnotation(AwsCredentialsProvider.class, AwsCredentialsProviderMixin.class);
    setMixInAnnotation(ProxyConfiguration.class, ProxyConfigurationMixin.class);
    setMixInAnnotation(AttributeMap.class, AttributeMapMixin.class);
    setMixInAnnotation(SSECustomerKey.class, SSECustomerKeyMixin.class);
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
      ObjectNode json =
          checkNotNull(
              jsonParser.readValueAs(new TypeReference<ObjectNode>() {}),
              "Serialized AWS credentials provider is null");

      String typeNameKey = typeDeserializer.getPropertyName();
      String typeName = getNotNull(json, typeNameKey, "unknown");
      json.remove(typeNameKey);

      if (hasName(StaticCredentialsProvider.class, typeName)) {
        boolean isSession = json.has(SESSION_TOKEN);
        if (isSession) {
          return StaticCredentialsProvider.create(
              AwsSessionCredentials.create(
                  getNotNull(json, ACCESS_KEY_ID, typeName),
                  getNotNull(json, SECRET_ACCESS_KEY, typeName),
                  getNotNull(json, SESSION_TOKEN, typeName)));
        } else {
          return StaticCredentialsProvider.create(
              AwsBasicCredentials.create(
                  getNotNull(json, ACCESS_KEY_ID, typeName),
                  getNotNull(json, SECRET_ACCESS_KEY, typeName)));
        }
      } else if (hasName(DefaultCredentialsProvider.class, typeName)) {
        return DefaultCredentialsProvider.create();
      } else if (hasName(EnvironmentVariableCredentialsProvider.class, typeName)) {
        return EnvironmentVariableCredentialsProvider.create();
      } else if (hasName(SystemPropertyCredentialsProvider.class, typeName)) {
        return SystemPropertyCredentialsProvider.create();
      } else if (hasName(ProfileCredentialsProvider.class, typeName)) {
        return ProfileCredentialsProvider.create();
      } else if (hasName(ContainerCredentialsProvider.class, typeName)) {
        return ContainerCredentialsProvider.builder().build();
      } else if (typeName.equals(StsAssumeRoleCredentialsProvider.class.getSimpleName())) {
        Class<? extends AssumeRoleRequest.Builder> clazz =
            AssumeRoleRequest.serializableBuilderClass();
        return StsAssumeRoleCredentialsProvider.builder()
            .refreshRequest(jsonParser.getCodec().treeToValue(json, clazz).build())
            .stsClient(StsClient.create())
            .build();
      } else {
        throw new IOException(
            String.format("AWS credential provider type '%s' is not supported", typeName));
      }
    }

    private String getNotNull(JsonNode json, String key, String typeName) {
      JsonNode node = json.get(key);
      checkNotNull(node, "AWS credentials provider type '%s' is missing '%s'", typeName, key);
      return node.textValue();
    }

    private boolean hasName(Class<? extends AwsCredentialsProvider> clazz, String typeName) {
      return clazz.getSimpleName().equals(typeName);
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
      // BEAM-11958 Use deprecated Jackson APIs to be compatible with older versions of jackson
      typeSerializer.writeTypePrefixForObject(credentialsProvider, jsonGenerator);
      Class<?> providerClass = credentialsProvider.getClass();
      if (providerClass.equals(StaticCredentialsProvider.class)) {
        AwsCredentials credentials = credentialsProvider.resolveCredentials();
        if (credentials.getClass().equals(AwsSessionCredentials.class)) {
          AwsSessionCredentials sessionCredentials = (AwsSessionCredentials) credentials;
          jsonGenerator.writeStringField(ACCESS_KEY_ID, sessionCredentials.accessKeyId());
          jsonGenerator.writeStringField(SECRET_ACCESS_KEY, sessionCredentials.secretAccessKey());
          jsonGenerator.writeStringField(SESSION_TOKEN, sessionCredentials.sessionToken());
        } else {
          jsonGenerator.writeStringField(ACCESS_KEY_ID, credentials.accessKeyId());
          jsonGenerator.writeStringField(SECRET_ACCESS_KEY, credentials.secretAccessKey());
        }
      } else if (providerClass.equals(StsAssumeRoleCredentialsProvider.class)) {
        Supplier<AssumeRoleRequest> reqSupplier =
            (Supplier<AssumeRoleRequest>)
                readField(credentialsProvider, "assumeRoleRequestSupplier");
        serializer
            .findValueSerializer(AssumeRoleRequest.serializableBuilderClass())
            .unwrappingSerializer(NameTransformer.NOP)
            .serialize(reqSupplier.get().toBuilder(), jsonGenerator, serializer);
      } else if (!SINGLETON_CREDENTIAL_PROVIDERS.contains(providerClass)) {
        throw new IllegalArgumentException(
            "Unsupported AWS credentials provider type " + providerClass);
      }
      // BEAM-11958 Use deprecated Jackson APIs to be compatible with older versions of jackson
      typeSerializer.writeTypeSuffixForObject(credentialsProvider, jsonGenerator);
    }

    private Object readField(AwsCredentialsProvider provider, String fieldName) throws IOException {
      try {
        return FieldUtils.readField(provider, fieldName, true);
      } catch (IllegalArgumentException | IllegalAccessException e) {
        throw new IOException(
            String.format(
                "Failed to access private field '%s' of AWS credential provider type '%s' with reflection",
                fieldName, provider.getClass().getSimpleName()),
            e);
      }
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
          checkNotNull(
              jsonParser.readValueAs(new TypeReference<Map<String, String>>() {}),
              "Serialized ProxyConfiguration is null");

      ProxyConfiguration.Builder builder = ProxyConfiguration.builder();
      final String endpoint = asMap.get("endpoint");
      if (endpoint != null) {
        builder.endpoint(URI.create(endpoint));
      }
      final String username = asMap.get("username");
      if (username != null) {
        builder.username(username);
      }
      final String password = asMap.get("password");
      if (password != null) {
        builder.password(password);
      }
      // defaults to FALSE / disabled
      Boolean useSystemPropertyValues = Boolean.valueOf(asMap.get("useSystemPropertyValues"));
      return builder.useSystemPropertyValues(useSystemPropertyValues).build();
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

  /** A mixin to add Jackson annotations to {@link AttributeMap}. */
  @JsonSerialize(using = AttributeMapSerializer.class)
  @JsonDeserialize(using = AttributeMapDeserializer.class)
  private static class AttributeMapMixin {}

  private static class AttributeMapDeserializer extends JsonDeserializer<AttributeMap> {
    @Override
    public AttributeMap deserialize(JsonParser jsonParser, DeserializationContext context)
        throws IOException {
      Map<String, String> map =
          checkNotNull(
              jsonParser.readValueAs(new TypeReference<Map<String, String>>() {}),
              "Serialized AttributeMap is null");

      // Add new attributes below.
      final AttributeMap.Builder attributeMapBuilder = AttributeMap.builder();
      if (map.containsKey(CONNECTION_ACQUIRE_TIMEOUT)) {
        attributeMapBuilder.put(
            SdkHttpConfigurationOption.CONNECTION_ACQUIRE_TIMEOUT,
            Duration.parse(map.get(CONNECTION_ACQUIRE_TIMEOUT)));
      }
      if (map.containsKey(CONNECTION_MAX_IDLE_TIMEOUT)) {
        attributeMapBuilder.put(
            SdkHttpConfigurationOption.CONNECTION_MAX_IDLE_TIMEOUT,
            Duration.parse(map.get(CONNECTION_MAX_IDLE_TIMEOUT)));
      }
      if (map.containsKey(CONNECTION_TIMEOUT)) {
        attributeMapBuilder.put(
            SdkHttpConfigurationOption.CONNECTION_TIMEOUT,
            Duration.parse(map.get(CONNECTION_TIMEOUT)));
      }
      if (map.containsKey(CONNECTION_TIME_TO_LIVE)) {
        attributeMapBuilder.put(
            SdkHttpConfigurationOption.CONNECTION_TIME_TO_LIVE,
            Duration.parse(map.get(CONNECTION_TIME_TO_LIVE)));
      }
      if (map.containsKey(MAX_CONNECTIONS)) {
        attributeMapBuilder.put(
            SdkHttpConfigurationOption.MAX_CONNECTIONS, Integer.parseInt(map.get(MAX_CONNECTIONS)));
      }
      if (map.containsKey(READ_TIMEOUT)) {
        attributeMapBuilder.put(
            SdkHttpConfigurationOption.READ_TIMEOUT, Duration.parse(map.get(READ_TIMEOUT)));
      }
      return attributeMapBuilder.build();
    }
  }

  private static class AttributeMapSerializer extends JsonSerializer<AttributeMap> {

    @Override
    public void serialize(
        AttributeMap attributeMap, JsonGenerator jsonGenerator, SerializerProvider serializer)
        throws IOException {

      jsonGenerator.writeStartObject();
      if (attributeMap.containsKey(SdkHttpConfigurationOption.CONNECTION_ACQUIRE_TIMEOUT)) {
        jsonGenerator.writeStringField(
            CONNECTION_ACQUIRE_TIMEOUT,
            String.valueOf(
                attributeMap.get(SdkHttpConfigurationOption.CONNECTION_ACQUIRE_TIMEOUT)));
      }
      if (attributeMap.containsKey(SdkHttpConfigurationOption.CONNECTION_MAX_IDLE_TIMEOUT)) {
        jsonGenerator.writeStringField(
            CONNECTION_MAX_IDLE_TIMEOUT,
            String.valueOf(
                attributeMap.get(SdkHttpConfigurationOption.CONNECTION_MAX_IDLE_TIMEOUT)));
      }
      if (attributeMap.containsKey(SdkHttpConfigurationOption.CONNECTION_TIMEOUT)) {
        jsonGenerator.writeStringField(
            CONNECTION_TIMEOUT,
            String.valueOf(attributeMap.get(SdkHttpConfigurationOption.CONNECTION_TIMEOUT)));
      }
      if (attributeMap.containsKey(SdkHttpConfigurationOption.CONNECTION_TIME_TO_LIVE)) {
        jsonGenerator.writeStringField(
            CONNECTION_TIME_TO_LIVE,
            String.valueOf(attributeMap.get(SdkHttpConfigurationOption.CONNECTION_TIME_TO_LIVE)));
      }
      if (attributeMap.containsKey(SdkHttpConfigurationOption.MAX_CONNECTIONS)) {
        jsonGenerator.writeStringField(
            MAX_CONNECTIONS,
            String.valueOf(attributeMap.get(SdkHttpConfigurationOption.MAX_CONNECTIONS)));
      }
      if (attributeMap.containsKey(SdkHttpConfigurationOption.READ_TIMEOUT)) {
        jsonGenerator.writeStringField(
            READ_TIMEOUT,
            String.valueOf(attributeMap.get(SdkHttpConfigurationOption.READ_TIMEOUT)));
      }
      jsonGenerator.writeEndObject();
    }
  }

  @JsonDeserialize(using = SSECustomerKeyDeserializer.class)
  private static class SSECustomerKeyMixin {}

  private static class SSECustomerKeyDeserializer extends JsonDeserializer<SSECustomerKey> {

    @Override
    public SSECustomerKey deserialize(JsonParser parser, DeserializationContext context)
        throws IOException {
      Map<String, String> asMap =
          checkNotNull(
              parser.readValueAs(new TypeReference<Map<String, String>>() {}),
              "Serialized SSECustomerKey is null");

      final String key = getNotNull(asMap, "key");
      final String algorithm = getNotNull(asMap, "algorithm");
      SSECustomerKey.Builder builder = SSECustomerKey.builder().key(key).algorithm(algorithm);

      final String md5 = asMap.get("md5");
      if (md5 != null) {
        builder.md5(md5);
      }
      return builder.build();
    }

    @SuppressWarnings({"nullness"})
    private String getNotNull(Map<String, String> map, String key) {
      return checkNotNull(
          map.get(key), "Encryption %s is missing in serialized SSECustomerKey", key);
    }
  }
}
