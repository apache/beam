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
import com.azure.core.util.Configuration;
import com.azure.identity.*;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.type.WritableTypeId;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.Map;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;

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

  public AzureModule() {
    super("AzureModule");
    setMixInAnnotation(TokenCredential.class, TokenCredentialMixin.class);
    setMixInAnnotation(Configuration.class, AzureClientConfigurationMixin.class);
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

      String typeNameKey = typeDeserializer.getPropertyName();
      String typeName = asMap.get(typeNameKey);
      if (typeName == null) {
        throw new IOException(
            String.format("Azure credentials provider type name key '%s' not found", typeNameKey));
      }

      if (typeName.equals(ClientSecretCredential.class.getSimpleName())) {
        return new ClientSecretCredentialBuilder()
            .clientId(asMap.get(AZURE_CLIENT_ID))
            .clientSecret(asMap.get(AZURE_CLIENT_SECRET))
            .tenantId(asMap.get(AZURE_TENANT_ID))
            .build();
      } else if (typeName.equals(ManagedIdentityCredential.class.getSimpleName())) {
        return new ManagedIdentityCredentialBuilder().clientId(asMap.get(AZURE_CLIENT_ID)).build();
      } else if (typeName.equals(EnvironmentCredential.class.getSimpleName())) {
        return new EnvironmentCredentialBuilder().build();
      } else if (typeName.equals(ClientCertificateCredential.class.getSimpleName())) {
        return new ClientCertificateCredentialBuilder()
            .clientId(asMap.get(AZURE_CLIENT_ID))
            // This could be a PFX certificate instead...
            // .pfxCertificate("<PATH TO PFX CERTIFICATE>", "PFX CERTIFICATE PASSWORD")
            .pemCertificate(asMap.get(AZURE_CLIENT_CERTIFICATE_PATH))
            .tenantId(asMap.get(AZURE_TENANT_ID))
            .build();
      } else if (typeName.equals(UsernamePasswordCredential.class.getSimpleName())) {
        return new UsernamePasswordCredentialBuilder()
            .clientId(asMap.get(AZURE_CLIENT_ID))
            .username(asMap.get(AZURE_USERNAME))
            .password(asMap.get(AZURE_PASSWORD))
            .build();
        // Add other credentials...
      } else {
        throw new IOException(
            String.format("Azure credential provider type '%s' is not supported", typeName));
      }
    }
  }

  private static class TokenCredentialSerializer extends JsonSerializer<TokenCredential> {
    // These providers are singletons, so don't require any serialization, other than type.
    private static final ImmutableSet<Object> SINGLETON_CREDENTIAL_PROVIDERS =
        // add any singleton credentials...
        ImmutableSet.of(DefaultAzureCredential.class);

    @Override
    public void serialize(
        TokenCredential tokenCredential,
        JsonGenerator jsonGenerator,
        SerializerProvider serializers)
        throws IOException {
      serializers.defaultSerializeValue(tokenCredential, jsonGenerator);
    }

    @Override
    public void serializeWithType(
        TokenCredential tokenCredential,
        JsonGenerator jsonGenerator,
        SerializerProvider serializers,
        TypeSerializer typeSerializer)
        throws IOException {
      WritableTypeId typeId =
          typeSerializer.writeTypePrefix(
              jsonGenerator, typeSerializer.typeId(tokenCredential, JsonToken.START_OBJECT));
      // add different credentials...
      if (!SINGLETON_CREDENTIAL_PROVIDERS.contains(tokenCredential.getClass())) {
        throw new IllegalArgumentException(
            "Unsupported Azure credentials provider type " + tokenCredential.getClass());
      }
      typeSerializer.writeTypeSuffix(jsonGenerator, typeId);
    }
  }

  /** A mixin to add Jackson annotations to {@link Configuration}. */
  @JsonSerialize(using = AzureClientConfigurationSerializer.class)
  @JsonDeserialize(using = AzureClientConfigurationDeserializer.class)
  private static class AzureClientConfigurationMixin {}

  private static class AzureClientConfigurationDeserializer
      extends JsonDeserializer<Configuration> {
    @Override
    public Configuration deserialize(JsonParser jsonParser, DeserializationContext context)
        throws IOException {
      Map<String, Object> map = jsonParser.readValueAs(new TypeReference<Map<String, Object>>() {});

      Configuration clientConfiguration = new Configuration();
      // add the options...

      return clientConfiguration;
    }
  }

  private static class AzureClientConfigurationSerializer extends JsonSerializer<Configuration> {

    @Override
    public void serialize(
        Configuration clientConfiguration,
        JsonGenerator jsonGenerator,
        SerializerProvider serializer)
        throws IOException {

      jsonGenerator.writeStartObject();
      // add options...
      jsonGenerator.writeEndObject();
    }
  }
}
