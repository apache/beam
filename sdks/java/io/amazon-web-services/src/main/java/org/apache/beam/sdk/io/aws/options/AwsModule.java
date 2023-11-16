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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
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
import com.fasterxml.jackson.core.type.TypeReference;
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
import java.util.Map;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;

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
  private static final String SESSION_TOKEN = "sessionToken";
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

  @SuppressWarnings({"nullness"})
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
          checkNotNull(
              jsonParser.readValueAs(new TypeReference<Map<String, String>>() {}),
              "Serialized AWS credentials provider is null");

      String typeNameKey = typeDeserializer.getPropertyName();
      String typeName = getNotNull(asMap, typeNameKey, "unknown");

      if (hasName(AWSStaticCredentialsProvider.class, typeName)) {
        boolean isSession = asMap.containsKey(SESSION_TOKEN);
        if (isSession) {
          return new AWSStaticCredentialsProvider(
              new BasicSessionCredentials(
                  getNotNull(asMap, AWS_ACCESS_KEY_ID, typeName),
                  getNotNull(asMap, AWS_SECRET_KEY, typeName),
                  getNotNull(asMap, SESSION_TOKEN, typeName)));
        } else {
          return new AWSStaticCredentialsProvider(
              new BasicAWSCredentials(
                  getNotNull(asMap, AWS_ACCESS_KEY_ID, typeName),
                  getNotNull(asMap, AWS_SECRET_KEY, typeName)));
        }
      } else if (hasName(PropertiesFileCredentialsProvider.class, typeName)) {
        return new PropertiesFileCredentialsProvider(
            getNotNull(asMap, CREDENTIALS_FILE_PATH, typeName));
      } else if (hasName(ClasspathPropertiesFileCredentialsProvider.class, typeName)) {
        return new ClasspathPropertiesFileCredentialsProvider(
            getNotNull(asMap, CREDENTIALS_FILE_PATH, typeName));
      } else if (hasName(DefaultAWSCredentialsProviderChain.class, typeName)) {
        return new DefaultAWSCredentialsProviderChain();
      } else if (hasName(EnvironmentVariableCredentialsProvider.class, typeName)) {
        return new EnvironmentVariableCredentialsProvider();
      } else if (hasName(SystemPropertiesCredentialsProvider.class, typeName)) {
        return new SystemPropertiesCredentialsProvider();
      } else if (hasName(ProfileCredentialsProvider.class, typeName)) {
        return new ProfileCredentialsProvider();
      } else if (hasName(EC2ContainerCredentialsProviderWrapper.class, typeName)) {
        return new EC2ContainerCredentialsProviderWrapper();
      } else if (hasName(STSAssumeRoleSessionCredentialsProvider.class, typeName)) {
        return new STSAssumeRoleSessionCredentialsProvider.Builder(
                getNotNull(asMap, ROLE_ARN, typeName),
                getNotNull(asMap, ROLE_SESSION_NAME, typeName))
            .build();
      } else {
        throw new IOException(
            String.format("AWS credential provider type '%s' is not supported", typeName));
      }
    }

    @SuppressWarnings({"nullness"})
    private String getNotNull(Map<String, String> map, String key, String typeName) {
      return checkNotNull(
          map.get(key), "AWS credentials provider type '%s' is missing '%s'", typeName, key);
    }

    private boolean hasName(Class<? extends AWSCredentialsProvider> clazz, String typeName) {
      return clazz.getSimpleName().equals(typeName);
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
            EC2ContainerCredentialsProviderWrapper.class);

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
      // BEAM-11958 Use deprecated Jackson APIs to be compatible with older versions of jackson
      typeSerializer.writeTypePrefixForObject(credentialsProvider, jsonGenerator);

      Class<?> providerClass = credentialsProvider.getClass();
      if (providerClass.equals(AWSStaticCredentialsProvider.class)) {
        AWSCredentials credentials = credentialsProvider.getCredentials();
        if (credentials.getClass().equals(BasicSessionCredentials.class)) {
          BasicSessionCredentials sessionCredentials = (BasicSessionCredentials) credentials;
          jsonGenerator.writeStringField(AWS_ACCESS_KEY_ID, sessionCredentials.getAWSAccessKeyId());
          jsonGenerator.writeStringField(AWS_SECRET_KEY, sessionCredentials.getAWSSecretKey());
          jsonGenerator.writeStringField(SESSION_TOKEN, sessionCredentials.getSessionToken());
        } else {
          jsonGenerator.writeStringField(AWS_ACCESS_KEY_ID, credentials.getAWSAccessKeyId());
          jsonGenerator.writeStringField(AWS_SECRET_KEY, credentials.getAWSSecretKey());
        }
      } else if (providerClass.equals(PropertiesFileCredentialsProvider.class)) {
        String filePath = (String) readField(credentialsProvider, CREDENTIALS_FILE_PATH);
        jsonGenerator.writeStringField(CREDENTIALS_FILE_PATH, filePath);
      } else if (providerClass.equals(ClasspathPropertiesFileCredentialsProvider.class)) {
        String filePath = (String) readField(credentialsProvider, CREDENTIALS_FILE_PATH);
        jsonGenerator.writeStringField(CREDENTIALS_FILE_PATH, filePath);
      } else if (providerClass.equals(STSAssumeRoleSessionCredentialsProvider.class)) {
        String arn = (String) readField(credentialsProvider, ROLE_ARN);
        String sessionName = (String) readField(credentialsProvider, ROLE_SESSION_NAME);
        jsonGenerator.writeStringField(ROLE_ARN, arn);
        jsonGenerator.writeStringField(ROLE_SESSION_NAME, sessionName);
      } else if (!SINGLETON_CREDENTIAL_PROVIDERS.contains(providerClass)) {
        throw new IllegalArgumentException(
            "Unsupported AWS credentials provider type " + providerClass);
      }
      // BEAM-11958 Use deprecated Jackson APIs to be compatible with older versions of jackson
      typeSerializer.writeTypeSuffixForObject(credentialsProvider, jsonGenerator);
    }

    private Object readField(AWSCredentialsProvider provider, String fieldName) throws IOException {
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

  @SuppressWarnings({"nullness"})
  private static String getNotNull(Map<String, String> map, String key, Class<?> clazz) {
    return checkNotNull(map.get(key), "`%s` required in serialized %s", key, clazz.getSimpleName());
  }

  /** A mixin to add Jackson annotations to {@link SSECustomerKey}. */
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

      SSECustomerKey sseCustomerKey =
          new SSECustomerKey(getNotNull(asMap, "key", SSECustomerKey.class));
      final String algorithm = asMap.get("algorithm");
      final String md5 = asMap.get("md5");
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
      Map<String, String> asMap =
          checkNotNull(
              parser.readValueAs(new TypeReference<Map<String, String>>() {}),
              "Serialized SSEAwsKeyManagementParams is null");

      return new SSEAwsKeyManagementParams(
          getNotNull(asMap, "awsKmsKeyId", SSEAwsKeyManagementParams.class));
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
      Map<String, Object> map =
          checkNotNull(
              jsonParser.readValueAs(new TypeReference<Map<String, Object>>() {}),
              "Serialized ClientConfiguration is null");

      ClientConfiguration clientConfiguration = new ClientConfiguration();

      if (map.containsKey(PROXY_HOST)) {
        clientConfiguration.setProxyHost((String) map.get(PROXY_HOST));
      }
      if (map.containsKey(PROXY_PORT)) {
        clientConfiguration.setProxyPort(((Number) map.get(PROXY_PORT)).intValue());
      }
      if (map.containsKey(PROXY_USERNAME)) {
        clientConfiguration.setProxyUsername((String) map.get(PROXY_USERNAME));
      }
      if (map.containsKey(PROXY_PASSWORD)) {
        clientConfiguration.setProxyPassword((String) map.get(PROXY_PASSWORD));
      }
      if (map.containsKey(CLIENT_EXECUTION_TIMEOUT)) {
        clientConfiguration.setClientExecutionTimeout(
            ((Number) map.get(CLIENT_EXECUTION_TIMEOUT)).intValue());
      }
      if (map.containsKey(CONNECTION_MAX_IDLE_TIME)) {
        clientConfiguration.setConnectionMaxIdleMillis(
            ((Number) map.get(CONNECTION_MAX_IDLE_TIME)).longValue());
      }
      if (map.containsKey(CONNECTION_TIMEOUT)) {
        clientConfiguration.setConnectionTimeout(((Number) map.get(CONNECTION_TIMEOUT)).intValue());
      }
      if (map.containsKey(CONNECTION_TIME_TO_LIVE)) {
        clientConfiguration.setConnectionTTL(
            ((Number) map.get(CONNECTION_TIME_TO_LIVE)).longValue());
      }
      if (map.containsKey(MAX_CONNECTIONS)) {
        clientConfiguration.setMaxConnections(((Number) map.get(MAX_CONNECTIONS)).intValue());
      }
      if (map.containsKey(REQUEST_TIMEOUT)) {
        clientConfiguration.setRequestTimeout(((Number) map.get(REQUEST_TIMEOUT)).intValue());
      }
      if (map.containsKey(SOCKET_TIMEOUT)) {
        clientConfiguration.setSocketTimeout(((Number) map.get(SOCKET_TIMEOUT)).intValue());
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
