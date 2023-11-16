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
package org.apache.beam.sdk.io.kinesis.serde;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

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
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
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
import java.io.IOException;
import java.util.Map;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.reflect.FieldUtils;

/**
 * A Jackson {@link Module} that registers a {@link JsonSerializer} and {@link JsonDeserializer} for
 * {@link AWSCredentialsProvider} and some subclasses. The serialized form is a JSON map.
 *
 * <p>Note: This module is a stripped down version of {@link AwsModule} in 'amazon-web-services'
 * excluding support for STS.
 */
class AwsModule extends SimpleModule {

  private static final String AWS_ACCESS_KEY_ID = "awsAccessKeyId";
  private static final String AWS_SECRET_KEY = "awsSecretKey";
  private static final String SESSION_TOKEN = "sessionToken";
  private static final String CREDENTIALS_FILE_PATH = "credentialsFilePath";

  @SuppressWarnings({"nullness"})
  AwsModule() {
    super("AwsModule");
    setMixInAnnotation(AWSCredentialsProvider.class, AWSCredentialsProviderMixin.class);
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
          checkNotNull(jsonParser.readValueAs(new TypeReference<Map<String, String>>() {}));

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
        return DefaultAWSCredentialsProviderChain.getInstance();
      } else if (hasName(EnvironmentVariableCredentialsProvider.class, typeName)) {
        return new EnvironmentVariableCredentialsProvider();
      } else if (hasName(SystemPropertiesCredentialsProvider.class, typeName)) {
        return new SystemPropertiesCredentialsProvider();
      } else if (hasName(ProfileCredentialsProvider.class, typeName)) {
        return new ProfileCredentialsProvider();
      } else if (hasName(EC2ContainerCredentialsProviderWrapper.class, typeName)) {
        return new EC2ContainerCredentialsProviderWrapper();
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
      return typeName.equals(clazz.getSimpleName());
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
        jsonGenerator.writeStringField(
            CREDENTIALS_FILE_PATH, readProviderField(credentialsProvider, CREDENTIALS_FILE_PATH));
      } else if (providerClass.equals(ClasspathPropertiesFileCredentialsProvider.class)) {
        jsonGenerator.writeStringField(
            CREDENTIALS_FILE_PATH, readProviderField(credentialsProvider, CREDENTIALS_FILE_PATH));
      } else if (!SINGLETON_CREDENTIAL_PROVIDERS.contains(credentialsProvider.getClass())) {
        throw new IllegalArgumentException(
            "Unsupported AWS credentials provider type " + credentialsProvider.getClass());
      }
      // BEAM-11958 Use deprecated Jackson APIs to be compatible with older versions of jackson
      typeSerializer.writeTypeSuffixForObject(credentialsProvider, jsonGenerator);
    }

    private String readProviderField(AWSCredentialsProvider provider, String fieldName)
        throws IOException {
      try {
        return (String) checkNotNull(FieldUtils.readField(provider, fieldName, true));
      } catch (NullPointerException | IllegalArgumentException | IllegalAccessException e) {
        throw new IOException(
            String.format(
                "Failed to access private field '%s' of AWS credential provider type '%s' with reflection",
                fieldName, provider.getClass().getSimpleName()),
            e);
      }
    }
  }
}
