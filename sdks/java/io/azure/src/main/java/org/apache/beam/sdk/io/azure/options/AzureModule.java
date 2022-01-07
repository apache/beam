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
package org.apache.beam.sdk.io.azure.options;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.ClientCertificateCredential;
import com.azure.identity.ClientCertificateCredentialBuilder;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.identity.EnvironmentCredential;
import com.azure.identity.EnvironmentCredentialBuilder;
import com.azure.identity.ManagedIdentityCredential;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.identity.UsernamePasswordCredential;
import com.azure.identity.UsernamePasswordCredentialBuilder;
import com.azure.identity.implementation.IdentityClient;
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

/**
 * A Jackson {@link Module} that registers a {@link JsonSerializer} and {@link JsonDeserializer} for
 * Azure credential providers. The serialized form is a JSON map.
 */
@AutoService(Module.class)
public class AzureModule extends SimpleModule {

  private static final String AZURE_CLIENT_ID = "azureClientId";
  private static final String AZURE_TENANT_ID = "azureTenantId";
  private static final String AZURE_CLIENT_SECRET = "azureClientSecret";
  private static final String AZURE_CLIENT_CERTIFICATE_PATH = "azureClientCertificatePath";
  private static final String AZURE_USERNAME = "azureUsername";
  private static final String AZURE_PASSWORD = "azurePassword";
  private static final String AZURE_PFX_CERTIFICATE_PATH = "azurePfxCertificatePath";
  private static final String AZURE_PFX_CERTIFICATE_PASSWORD = "azurePfxCertificatePassword";

  @SuppressWarnings({
    "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
  })
  public AzureModule() {
    super("AzureModule");
    setMixInAnnotation(TokenCredential.class, TokenCredentialMixin.class);
  }

  /** A mixin to add Jackson annotations to {@link TokenCredential}. */
  @JsonDeserialize(using = TokenCredentialDeserializer.class)
  @JsonSerialize(using = TokenCredentialSerializer.class)
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
  private static class TokenCredentialMixin {}

  private static class TokenCredentialDeserializer extends JsonDeserializer<TokenCredential> {

    @Override
    public TokenCredential deserialize(JsonParser jsonParser, DeserializationContext context)
        throws IOException {
      return context.readValue(jsonParser, TokenCredential.class);
    }

    @Override
    public TokenCredential deserializeWithType(
        JsonParser jsonParser, DeserializationContext context, TypeDeserializer typeDeserializer)
        throws IOException {
      Map<String, String> asMap =
          jsonParser.readValueAs(new TypeReference<Map<String, String>>() {});
      if (asMap == null) {
        throw new IOException("Azure credentials provider could not be read.");
      }

      String typeNameKey = typeDeserializer.getPropertyName();
      String typeName = asMap.get(typeNameKey);
      if (typeName == null) {
        throw new IOException(
            String.format("Azure credentials provider type name key '%s' not found", typeNameKey));
      }

      if (typeName.equals(DefaultAzureCredential.class.getSimpleName())) {
        return new DefaultAzureCredentialBuilder().build();
      } else if (typeName.equals(ClientSecretCredential.class.getSimpleName())) {
        return new ClientSecretCredentialBuilder()
            .clientId(asMap.getOrDefault(AZURE_CLIENT_ID, ""))
            .clientSecret(asMap.getOrDefault(AZURE_CLIENT_SECRET, ""))
            .tenantId(asMap.getOrDefault(AZURE_TENANT_ID, ""))
            .build();
      } else if (typeName.equals(ManagedIdentityCredential.class.getSimpleName())) {
        return new ManagedIdentityCredentialBuilder()
            .clientId(asMap.getOrDefault(AZURE_CLIENT_ID, ""))
            .build();
      } else if (typeName.equals(EnvironmentCredential.class.getSimpleName())) {
        return new EnvironmentCredentialBuilder().build();
      } else if (typeName.equals(ClientCertificateCredential.class.getSimpleName())) {
        if (asMap.containsKey(AZURE_CLIENT_CERTIFICATE_PATH)) {
          return new ClientCertificateCredentialBuilder()
              .clientId(asMap.getOrDefault(AZURE_CLIENT_ID, ""))
              .pemCertificate(asMap.getOrDefault(AZURE_CLIENT_CERTIFICATE_PATH, ""))
              .tenantId(asMap.getOrDefault(AZURE_TENANT_ID, ""))
              .build();
        } else {
          return new ClientCertificateCredentialBuilder()
              .clientId(asMap.getOrDefault(AZURE_CLIENT_ID, ""))
              .pfxCertificate(
                  asMap.getOrDefault(AZURE_PFX_CERTIFICATE_PATH, ""),
                  asMap.getOrDefault(AZURE_PFX_CERTIFICATE_PASSWORD, ""))
              .tenantId(asMap.getOrDefault(AZURE_TENANT_ID, ""))
              .build();
        }
      } else if (typeName.equals(UsernamePasswordCredential.class.getSimpleName())) {
        return new UsernamePasswordCredentialBuilder()
            .clientId(asMap.getOrDefault(AZURE_CLIENT_ID, ""))
            .username(asMap.getOrDefault(AZURE_USERNAME, ""))
            .password(asMap.getOrDefault(AZURE_PASSWORD, ""))
            .tenantId(asMap.getOrDefault(AZURE_TENANT_ID, ""))
            .build();
      } else {
        throw new IOException(
            String.format("Azure credential provider type '%s' is not supported", typeName));
      }
    }
  }

  private static class TokenCredentialSerializer extends JsonSerializer<TokenCredential> {
    @Override
    public void serialize(
        TokenCredential tokenCredential,
        JsonGenerator jsonGenerator,
        SerializerProvider serializers)
        throws IOException {
      serializers.defaultSerializeValue(tokenCredential, jsonGenerator);
    }

    @SuppressWarnings("nullness")
    private static Object getMember(Object obj, String member)
        throws IllegalAccessException, NoSuchFieldException {
      Class<?> cls = obj.getClass();
      Field field = cls.getDeclaredField(member);
      field.setAccessible(true);
      Object fieldObj = field.get(obj);
      assert fieldObj != null;
      return fieldObj;
    }

    @Override
    public void serializeWithType(
        TokenCredential tokenCredential,
        JsonGenerator jsonGenerator,
        SerializerProvider serializers,
        TypeSerializer typeSerializer)
        throws IOException {

      WritableTypeId typeIdDef =
          typeSerializer.writeTypePrefix(
              jsonGenerator, typeSerializer.typeId(tokenCredential, JsonToken.START_OBJECT));

      try {
        if (tokenCredential instanceof DefaultAzureCredential) {
          // Do nothing
        } else if (tokenCredential instanceof ClientSecretCredential) {
          ClientSecretCredential credential = (ClientSecretCredential) tokenCredential;
          IdentityClient identityClient = (IdentityClient) getMember(credential, "identityClient");
          jsonGenerator.writeStringField(
              AZURE_CLIENT_ID, (String) getMember(identityClient, "clientId"));
          jsonGenerator.writeStringField(
              AZURE_TENANT_ID, (String) getMember(identityClient, "tenantId"));
          jsonGenerator.writeStringField(
              AZURE_CLIENT_SECRET, (String) getMember(credential, "clientSecret"));
        } else if (tokenCredential instanceof ManagedIdentityCredential) {
          ManagedIdentityCredential credential = (ManagedIdentityCredential) tokenCredential;
          Object appServiceMsiCredential = getMember(credential, "appServiceMSICredential");
          IdentityClient identityClient =
              (IdentityClient) getMember(appServiceMsiCredential, "identityClient");
          jsonGenerator.writeStringField(
              AZURE_CLIENT_ID, (String) getMember(identityClient, "clientId"));
        } else if (tokenCredential instanceof EnvironmentCredential) {
          // Do nothing
        } else if (tokenCredential instanceof ClientCertificateCredential) {
          throw new IOException("Client certificates not yet implemented"); // TODO
        } else if (tokenCredential instanceof UsernamePasswordCredential) {
          UsernamePasswordCredential credential = (UsernamePasswordCredential) tokenCredential;
          IdentityClient identityClient = (IdentityClient) getMember(credential, "identityClient");
          jsonGenerator.writeStringField(
              AZURE_CLIENT_ID, (String) getMember(identityClient, "clientId"));
          jsonGenerator.writeStringField(
              AZURE_USERNAME, (String) getMember(credential, "username"));
          jsonGenerator.writeStringField(
              AZURE_PASSWORD, (String) getMember(credential, "password"));
        } else {
          throw new IOException(
              String.format(
                  "Azure credential provider type '%s' is not supported",
                  tokenCredential.getClass().getSimpleName()));
        }
      } catch (IllegalAccessException | NoSuchFieldException e) {
        throw new IOException(
            String.format(
                "Failed to serialize object of type '%s': %s",
                tokenCredential.getClass().getSimpleName(), e.toString()));
      }

      typeSerializer.writeTypeSuffix(jsonGenerator, typeIdDef);
    }
  }
}
