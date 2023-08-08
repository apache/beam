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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
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
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.ValueInstantiator;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.NameTransformer;
import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.function.Supplier;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
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
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.profiles.ProfileFileSystemSetting;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleWithWebIdentityCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityRequest;

/**
 * A Jackson {@link Module} that registers a {@link JsonSerializer} and {@link JsonDeserializer} for
 * {@link AwsCredentialsProvider} and some subclasses. The serialized form is a JSON map.
 */
@AutoService(Module.class)
public class AwsModule extends SimpleModule {
  private static final String ACCESS_KEY_ID = "accessKeyId";
  private static final String SECRET_ACCESS_KEY = "secretAccessKey";
  private static final String SESSION_TOKEN = "sessionToken";
  private static final String PROFILE_NAME = "profileName";

  public AwsModule() {
    super("AwsModule");
  }

  @Override
  public void setupModule(SetupContext cxt) {
    cxt.setMixInAnnotations(AwsCredentialsProvider.class, AwsCredentialsProviderMixin.class);
    cxt.setMixInAnnotations(ProxyConfiguration.class, ProxyConfigurationMixin.class);
    cxt.setMixInAnnotations(
        ProxyConfiguration.Builder.class, ProxyConfigurationMixin.Builder.class);
    cxt.setMixInAnnotations(Region.class, RegionMixin.class);

    addValueInstantiator(ProxyConfiguration.Builder.class, ProxyConfiguration::builder);
    super.setupModule(cxt);
  }

  @JsonDeserialize(using = RegionMixin.Deserializer.class)
  @JsonSerialize(using = RegionMixin.Serializer.class)
  private static class RegionMixin {
    private static class Deserializer extends JsonDeserializer<Region> {
      @Override
      public Region deserialize(JsonParser p, DeserializationContext cxt) throws IOException {
        return Region.of(p.readValueAs(String.class));
      }
    }

    private static class Serializer extends JsonSerializer<Region> {
      @Override
      public void serialize(Region value, JsonGenerator gen, SerializerProvider serializers)
          throws IOException {
        gen.writeString(value.id());
      }
    }
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
        return json.has(PROFILE_NAME)
            ? ProfileCredentialsProvider.create(getNotNull(json, PROFILE_NAME, typeName))
            : ProfileCredentialsProvider.create();
      } else if (hasName(ContainerCredentialsProvider.class, typeName)) {
        return ContainerCredentialsProvider.builder().build();
      } else if (typeName.equals(StsAssumeRoleCredentialsProvider.class.getSimpleName())) {
        Class<? extends AssumeRoleRequest.Builder> clazz =
            AssumeRoleRequest.serializableBuilderClass();
        return StsAssumeRoleCredentialsProvider.builder()
            .refreshRequest(jsonParser.getCodec().treeToValue(json, clazz).build())
            .stsClient(StsClient.create())
            .build();
      } else if (typeName.equals(
          StsAssumeRoleWithWebIdentityCredentialsProvider.class.getSimpleName())) {
        Class<? extends AssumeRoleWithWebIdentityRequest.Builder> clazz =
            AssumeRoleWithWebIdentityRequest.serializableBuilderClass();
        return StsAssumeRoleWithWebIdentityCredentialsProvider.builder()
            .refreshRequest(jsonParser.getCodec().treeToValue(json, clazz).build())
            .stsClient(
                StsClient.builder()
                    .region(Region.AWS_GLOBAL)
                    .credentialsProvider(AnonymousCredentialsProvider.create())
                    .build())
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
      } else if (providerClass.equals(ProfileCredentialsProvider.class)) {
        String profileName = (String) readField(credentialsProvider, PROFILE_NAME);
        String envProfileName = ProfileFileSystemSetting.AWS_PROFILE.getStringValueOrThrow();
        if (profileName != null && !profileName.equals(envProfileName)) {
          jsonGenerator.writeStringField(PROFILE_NAME, profileName);
        }
      } else if (providerClass.equals(StsAssumeRoleCredentialsProvider.class)) {
        Supplier<AssumeRoleRequest> reqSupplier =
            (Supplier<AssumeRoleRequest>)
                readField(credentialsProvider, "assumeRoleRequestSupplier");
        serializer
            .findValueSerializer(AssumeRoleRequest.serializableBuilderClass())
            .unwrappingSerializer(NameTransformer.NOP)
            .serialize(reqSupplier.get().toBuilder(), jsonGenerator, serializer);
      } else if (providerClass.equals(StsAssumeRoleWithWebIdentityCredentialsProvider.class)) {
        Supplier<AssumeRoleWithWebIdentityRequest> reqSupplier =
            (Supplier<AssumeRoleWithWebIdentityRequest>)
                readField(credentialsProvider, "assumeRoleWithWebIdentityRequest");
        serializer
            .findValueSerializer(AssumeRoleWithWebIdentityRequest.serializableBuilderClass())
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
  @JsonDeserialize(builder = ProxyConfiguration.Builder.class)
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
  @JsonIgnoreProperties(value = {"host", "port", "scheme"})
  @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
  private static class ProxyConfigurationMixin {
    @JsonPOJOBuilder(withPrefix = "")
    static class Builder {}
  }

  private <T> void addValueInstantiator(Class<T> clazz, Instantiator<T> instantiator) {
    addValueInstantiator(
        clazz,
        new ValueInstantiator.Base(clazz) {
          @Override
          public Object createUsingDefault(DeserializationContext cxt) {
            return instantiator.create();
          }

          @Override
          public boolean canCreateUsingDefault() {
            return true;
          }
        });
  }

  private interface Instantiator<T> {
    @NonNull
    T create();
  }
}
