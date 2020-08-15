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
package org.apache.beam.sdk.io.aws.options;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.services.s3.model.SSECustomerKey;
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
import java.lang.reflect.Field;
import java.util.Map;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;

/**
 * A Jackson {@link Module} that registers a {@link JsonSerializer} and {@link JsonDeserializer} for
 * {@link AWSCredentialsProvider} and some subclasses. The serialized form is a JSON map.
 *
 * <p>It also adds serializers for S3 encryption objects {@link SSECustomerKey} and {@link
 * SSEAwsKeyManagementParams}.
 */
@AutoService(Module.class)
public class AwsModule extends SimpleModule {

  private static final String AWS_ACCESS_KEY_ID = "awsAccessKeyId";
  private static final String AWS_SECRET_KEY = "awsSecretKey";
  private static final String CREDENTIALS_FILE_PATH = "credentialsFilePath";
  public static final String CLIENT_EXECUTION_TIMEOUT = "clientExecutionTimeout";
  public static final String CONNECTION_MAX_IDLE_TIME = "connectionMaxIdleTime";
  public static final String CONNECTION_TIMEOUT = "connectionTimeout";
  public static final String CONNECTION_TIME_TO_LIVE = "connectionTimeToLive";
  public static final String MAX_CONNECTIONS = "maxConnections";
  public static final String REQUEST_TIMEOUT = "requestTimeout";
  public static final String SOCKET_TIMEOUT = "socketTimeout";
  public static final String PROXY_HOST = "proxyHost";
  public static final String PROXY_PORT = "proxyPort";
  public static final String PROXY_USERNAME = "proxyUsername";
  public static final String PROXY_PASSWORD = "proxyPassword";
  private static final String ROLE_ARN = "roleArn";
  private static final String ROLE_SESSION_NAME = "roleSessionName";

  public AwsModule() {
    super("AwsModule");
    setMixInAnnotation(AWSCredentialsProvider.class, AWSCredentialsProviderMixin.class);
    setMixInAnnotation(SSECustomerKey.class, SSECustomerKeyMixin.class);
    setMixInAnnotation(SSEAwsKeyManagementParams.class, SSEAwsKeyManagementParamsMixin.class);
    setMixInAnnotation(ClientConfiguration.class, AwsHttpClientConfigurationMixin.class);
  }

  /** A mixin to add Jackson annotations to {@link AWSCredentialsProvider}. */
  @JsonDeserialize(using = AWSCredentialsProviderDeserializer.class)
  @JsonSerialize(using = AWSCredentialsProviderSerializer.class)
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
  private static class AWSCredentialsProviderMixin {}

  private static class AWSCredentialsProviderDeserializer
      extends JsonDeserializer<AWSCredentialsProvider> {

    @Override
    public AWSCredentialsProvider deserialize(JsonParser jsonParser, DeserializationContext context)
        throws IOException {
      return context.readValue(jsonParser, AWSCredentialsProvider.class);
    }

    @Override
    public AWSCredentialsProvider deserializeWithType(
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

      if (typeName.equals(AWSStaticCredentialsProvider.class.getSimpleName())) {
        return new AWSStaticCredentialsProvider(
            new BasicAWSCredentials(asMap.get(AWS_ACCESS_KEY_ID), asMap.get(AWS_SECRET_KEY)));
      } else if (typeName.equals(PropertiesFileCredentialsProvider.class.getSimpleName())) {
        return new PropertiesFileCredentialsProvider(asMap.get(CREDENTIALS_FILE_PATH));
      } else if (typeName.equals(
          ClasspathPropertiesFileCredentialsProvider.class.getSimpleName())) {
        return new ClasspathPropertiesFileCredentialsProvider(asMap.get(CREDENTIALS_FILE_PATH));
      } else if (typeName.equals(DefaultAWSCredentialsProviderChain.class.getSimpleName())) {
        return new DefaultAWSCredentialsProviderChain();
      } else if (typeName.equals(EnvironmentVariableCredentialsProvider.class.getSimpleName())) {
        return new EnvironmentVariableCredentialsProvider();
      } else if (typeName.equals(SystemPropertiesCredentialsProvider.class.getSimpleName())) {
        return new SystemPropertiesCredentialsProvider();
      } else if (typeName.equals(ProfileCredentialsProvider.class.getSimpleName())) {
        return new ProfileCredentialsProvider();
      } else if (typeName.equals(EC2ContainerCredentialsProviderWrapper.class.getSimpleName())) {
        return new EC2ContainerCredentialsProviderWrapper();
      } else if (typeName.equals(STSAssumeRoleSessionCredentialsProvider.class.getSimpleName())) {
        return new STSAssumeRoleSessionCredentialsProvider.Builder(
                asMap.get(ROLE_ARN), asMap.get(ROLE_SESSION_NAME))
            .build();
      } else {
        throw new IOException(
            String.format("AWS credential provider type '%s' is not supported", typeName));
      }
    }
  }

  private static class AWSCredentialsProviderSerializer
      extends JsonSerializer<AWSCredentialsProvider> {
    // These providers are singletons, so don't require any serialization, other than type.
    private static final ImmutableSet<Object> SINGLETON_CREDENTIAL_PROVIDERS =
        ImmutableSet.of(
            DefaultAWSCredentialsProviderChain.class,
            EnvironmentVariableCredentialsProvider.class,
            SystemPropertiesCredentialsProvider.class,
            ProfileCredentialsProvider.class,
            EC2ContainerCredentialsProviderWrapper.class,
            STSAssumeRoleSessionCredentialsProvider.class);

    @Override
    public void serialize(
        AWSCredentialsProvider credentialsProvider,
        JsonGenerator jsonGenerator,
        SerializerProvider serializers)
        throws IOException {
      serializers.defaultSerializeValue(credentialsProvider, jsonGenerator);
    }

    @Override
    public void serializeWithType(
        AWSCredentialsProvider credentialsProvider,
        JsonGenerator jsonGenerator,
        SerializerProvider serializers,
        TypeSerializer typeSerializer)
        throws IOException {
      WritableTypeId typeId =
          typeSerializer.writeTypePrefix(
              jsonGenerator, typeSerializer.typeId(credentialsProvider, JsonToken.START_OBJECT));
      if (credentialsProvider.getClass().equals(AWSStaticCredentialsProvider.class)) {
        jsonGenerator.writeStringField(
            AWS_ACCESS_KEY_ID, credentialsProvider.getCredentials().getAWSAccessKeyId());
        jsonGenerator.writeStringField(
            AWS_SECRET_KEY, credentialsProvider.getCredentials().getAWSSecretKey());

      } else if (credentialsProvider.getClass().equals(PropertiesFileCredentialsProvider.class)) {
        try {
          PropertiesFileCredentialsProvider specificProvider =
              (PropertiesFileCredentialsProvider) credentialsProvider;
          Field field =
              PropertiesFileCredentialsProvider.class.getDeclaredField(CREDENTIALS_FILE_PATH);
          field.setAccessible(true);
          String credentialsFilePath = (String) field.get(specificProvider);
          jsonGenerator.writeStringField(CREDENTIALS_FILE_PATH, credentialsFilePath);
        } catch (NoSuchFieldException | IllegalAccessException e) {
          throw new IOException("failed to access private field with reflection", e);
        }

      } else if (credentialsProvider
          .getClass()
          .equals(ClasspathPropertiesFileCredentialsProvider.class)) {
        try {
          ClasspathPropertiesFileCredentialsProvider specificProvider =
              (ClasspathPropertiesFileCredentialsProvider) credentialsProvider;
          Field field =
              ClasspathPropertiesFileCredentialsProvider.class.getDeclaredField(
                  CREDENTIALS_FILE_PATH);
          field.setAccessible(true);
          String credentialsFilePath = (String) field.get(specificProvider);
          jsonGenerator.writeStringField(CREDENTIALS_FILE_PATH, credentialsFilePath);
        } catch (NoSuchFieldException | IllegalAccessException e) {
          throw new IOException("failed to access private field with reflection", e);
        }
      } else if (credentialsProvider
          .getClass()
          .equals(STSAssumeRoleSessionCredentialsProvider.class)) {
        try {
          STSAssumeRoleSessionCredentialsProvider specificProvider =
              (STSAssumeRoleSessionCredentialsProvider) credentialsProvider;

          Field fieldRole =
              STSAssumeRoleSessionCredentialsProvider.class.getDeclaredField(ROLE_ARN);
          fieldRole.setAccessible(true);
          String roleArn = (String) fieldRole.get(specificProvider);
          jsonGenerator.writeStringField(ROLE_ARN, roleArn);

          Field fieldSession =
              STSAssumeRoleSessionCredentialsProvider.class.getDeclaredField(ROLE_SESSION_NAME);
          fieldSession.setAccessible(true);
          String roleSessionName = (String) fieldSession.get(specificProvider);
          jsonGenerator.writeStringField(ROLE_SESSION_NAME, roleSessionName);
        } catch (NoSuchFieldException | IllegalAccessException e) {
          throw new IOException("failed to access private field with reflection", e);
        }
      } else if (!SINGLETON_CREDENTIAL_PROVIDERS.contains(credentialsProvider.getClass())) {
        throw new IllegalArgumentException(
            "Unsupported AWS credentials provider type " + credentialsProvider.getClass());
      }
      typeSerializer.writeTypeSuffix(jsonGenerator, typeId);
    }
  }

  /** A mixin to add Jackson annotations to {@link SSECustomerKey}. */
  @JsonDeserialize(using = SSECustomerKeyDeserializer.class)
  private static class SSECustomerKeyMixin {}

  private static class SSECustomerKeyDeserializer extends JsonDeserializer<SSECustomerKey> {
    @Override
    public SSECustomerKey deserialize(JsonParser parser, DeserializationContext context)
        throws IOException {
      Map<String, String> asMap = parser.readValueAs(new TypeReference<Map<String, String>>() {});

      final String key = asMap.getOrDefault("key", null);
      final String algorithm = asMap.getOrDefault("algorithm", null);
      final String md5 = asMap.getOrDefault("md5", null);
      SSECustomerKey sseCustomerKey = new SSECustomerKey(key);
      if (algorithm != null) {
        sseCustomerKey.setAlgorithm(algorithm);
      }
      if (md5 != null) {
        sseCustomerKey.setMd5(md5);
      }
      return sseCustomerKey;
    }
  }

  /** A mixin to add Jackson annotations to {@link SSEAwsKeyManagementParams}. */
  @JsonDeserialize(using = SSEAwsKeyManagementParamsDeserializer.class)
  private static class SSEAwsKeyManagementParamsMixin {}

  private static class SSEAwsKeyManagementParamsDeserializer
      extends JsonDeserializer<SSEAwsKeyManagementParams> {
    @Override
    public SSEAwsKeyManagementParams deserialize(JsonParser parser, DeserializationContext context)
        throws IOException {
      Map<String, String> asMap = parser.readValueAs(new TypeReference<Map<String, String>>() {});
      final String awsKmsKeyId = asMap.getOrDefault("awsKmsKeyId", null);
      return new SSEAwsKeyManagementParams(awsKmsKeyId);
    }
  }

  /** A mixin to add Jackson annotations to {@link ClientConfiguration}. */
  @JsonSerialize(using = AwsHttpClientConfigurationSerializer.class)
  @JsonDeserialize(using = AwsHttpClientConfigurationDeserializer.class)
  private static class AwsHttpClientConfigurationMixin {}

  private static class AwsHttpClientConfigurationDeserializer
      extends JsonDeserializer<ClientConfiguration> {
    @Override
    public ClientConfiguration deserialize(JsonParser jsonParser, DeserializationContext context)
        throws IOException {
      Map<String, Object> map = jsonParser.readValueAs(new TypeReference<Map<String, Object>>() {});

      ClientConfiguration clientConfiguration = new ClientConfiguration();

      if (map.containsKey(PROXY_HOST)) {
        clientConfiguration.setProxyHost((String) map.get(PROXY_HOST));
      }
      if (map.containsKey(PROXY_PORT)) {
        clientConfiguration.setProxyPort((Integer) map.get(PROXY_PORT));
      }
      if (map.containsKey(PROXY_USERNAME)) {
        clientConfiguration.setProxyUsername((String) map.get(PROXY_USERNAME));
      }
      if (map.containsKey(PROXY_PASSWORD)) {
        clientConfiguration.setProxyPassword((String) map.get(PROXY_PASSWORD));
      }
      if (map.containsKey(CLIENT_EXECUTION_TIMEOUT)) {
        clientConfiguration.setClientExecutionTimeout((Integer) map.get(CLIENT_EXECUTION_TIMEOUT));
      }
      if (map.containsKey(CONNECTION_MAX_IDLE_TIME)) {
        clientConfiguration.setConnectionMaxIdleMillis(
            ((Number) map.get(CONNECTION_MAX_IDLE_TIME)).longValue());
      }
      if (map.containsKey(CONNECTION_TIMEOUT)) {
        clientConfiguration.setConnectionTimeout((Integer) map.get(CONNECTION_TIMEOUT));
      }
      if (map.containsKey(CONNECTION_TIME_TO_LIVE)) {
        clientConfiguration.setConnectionTTL(
            ((Number) map.get(CONNECTION_TIME_TO_LIVE)).longValue());
      }
      if (map.containsKey(MAX_CONNECTIONS)) {
        clientConfiguration.setMaxConnections((Integer) map.get(MAX_CONNECTIONS));
      }
      if (map.containsKey(REQUEST_TIMEOUT)) {
        clientConfiguration.setRequestTimeout((Integer) map.get(REQUEST_TIMEOUT));
      }
      if (map.containsKey(SOCKET_TIMEOUT)) {
        clientConfiguration.setSocketTimeout((Integer) map.get(SOCKET_TIMEOUT));
      }
      return clientConfiguration;
    }
  }

  private static class AwsHttpClientConfigurationSerializer
      extends JsonSerializer<ClientConfiguration> {

    @Override
    public void serialize(
        ClientConfiguration clientConfiguration,
        JsonGenerator jsonGenerator,
        SerializerProvider serializer)
        throws IOException {

      jsonGenerator.writeStartObject();
      jsonGenerator.writeObjectField(PROXY_HOST /*string*/, clientConfiguration.getProxyHost());
      jsonGenerator.writeObjectField(PROXY_PORT /*int*/, clientConfiguration.getProxyPort());
      jsonGenerator.writeObjectField(
          PROXY_USERNAME /*string*/, clientConfiguration.getProxyUsername());
      jsonGenerator.writeObjectField(
          PROXY_PASSWORD /*string*/, clientConfiguration.getProxyPassword());
      jsonGenerator.writeObjectField(
          CLIENT_EXECUTION_TIMEOUT /*int*/, clientConfiguration.getClientExecutionTimeout());
      jsonGenerator.writeObjectField(
          CONNECTION_MAX_IDLE_TIME /*long*/, clientConfiguration.getConnectionMaxIdleMillis());
      jsonGenerator.writeObjectField(
          CONNECTION_TIMEOUT /*int*/, clientConfiguration.getConnectionTimeout());
      jsonGenerator.writeObjectField(
          CONNECTION_TIME_TO_LIVE /*long*/, clientConfiguration.getConnectionTTL());
      jsonGenerator.writeObjectField(
          MAX_CONNECTIONS /*int*/, clientConfiguration.getMaxConnections());
      jsonGenerator.writeObjectField(
          REQUEST_TIMEOUT /*int*/, clientConfiguration.getRequestTimeout());
      jsonGenerator.writeObjectField(
          SOCKET_TIMEOUT /*int*/, clientConfiguration.getSocketTimeout());
      jsonGenerator.writeEndObject();
    }
  }
}
