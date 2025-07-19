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
package org.apache.beam.sdk.io.aws2.auth;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.google.auto.value.AutoValue;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Suppliers;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleWithWebIdentityCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityRequest;
import software.amazon.awssdk.utils.SdkAutoCloseable;

/**
 * An implementation of AwsCredentialsProvider that periodically sends an {@link
 * AssumeRoleWithWebIdentityRequest} to the AWS Security Token Service to maintain short-lived
 * sessions to use for authentication. In particular this class will use a {@link
 * StsAssumeRoleWithWebIdentityCredentialsProvider} instance as a delegate for the actual
 * implementation but it takes care of retrieving a refreshed web id token with every credential's
 * resolution request. This is created using builder().
 *
 * @see <a
 *     href="https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html">API
 *     reference</a>
 */
@AutoValue
@JsonSubTypes({
  @JsonSubTypes.Type(
      value = AutoValue_StsAssumeRoleForFederatedCredentialsProvider.class,
      name = "StsAssumeRoleForFederatedCredentialsProvider")
})
public abstract class StsAssumeRoleForFederatedCredentialsProvider
    implements AwsCredentialsProvider, SdkAutoCloseable {

  public static final Integer DEFAULT_SESSION_DURATION_SECS = 3600;

  abstract CredentialsProviderDelegate credentialsProviderDelegate();

  public abstract String audience();

  public abstract String assumedRoleArn();

  public abstract String webIdTokenProviderFQCN();

  @Nullable
  public abstract Integer sessionDurationSecs();

  @Override
  public AwsCredentials resolveCredentials() {
    return credentialsProviderDelegate().resolveCredentials();
  }

  @Override
  public void close() {
    credentialsProviderDelegate().close();
  }

  /**
   * Creates a builder for the type {@link StsAssumeRoleForFederatedCredentialsProvider}.
   *
   * @return an initialized builder instance.
   */
  public static StsAssumeRoleForFederatedCredentialsProvider.Builder builder() {
    return new AutoValue_StsAssumeRoleForFederatedCredentialsProvider.Builder();
  }

  /** Builder class for {@link StsAssumeRoleForFederatedCredentialsProvider}. */
  @AutoValue.Builder
  public abstract static class Builder {

    /**
     * Sets the role to be assumed by the authentication request.
     *
     * @param roleArn the AWS role ARN.
     * @return this builder instance.
     */
    public abstract Builder setAssumedRoleArn(String roleArn);

    /**
     * Sets the audience to be used for the web id token request.
     *
     * @param audience the audience value.
     * @return this builder instance.
     */
    public abstract Builder setAudience(String audience);

    /**
     * The fully qualified class name for the web id token provider. The class should be accessible
     * in the classpath.
     *
     * @param idTokenProviderFQCN the class name.
     * @return this builder instance.
     */
    public abstract Builder setWebIdTokenProviderFQCN(String idTokenProviderFQCN);

    /**
     * The session duration in seconds for the authentication request, by default this value is
     * 3600.
     *
     * @param durationSecs the duration in seconds.
     * @return this builder instance.
     */
    public abstract Builder setSessionDurationSecs(@Nullable Integer durationSecs);

    /**
     * For testing purposes, should not be set in a production setup.
     *
     * @param delegate The testing delegate to use.
     * @return this builder instance.
     */
    abstract Builder setCredentialsProviderDelegate(CredentialsProviderDelegate delegate);

    abstract Optional<CredentialsProviderDelegate> credentialsProviderDelegate();

    abstract String assumedRoleArn();

    abstract String audience();

    abstract String webIdTokenProviderFQCN();

    abstract Integer sessionDurationSecs();

    abstract StsAssumeRoleForFederatedCredentialsProvider autoBuild(); // not public

    /**
     * Validates and fully initializes a {@link StsAssumeRoleForFederatedCredentialsProvider}
     * instance.
     *
     * @return the initialized credentials provider instance.
     */
    public StsAssumeRoleForFederatedCredentialsProvider build() {
      checkState(audience() != null, "Audience value should not be null");
      checkState(assumedRoleArn() != null, "The role to assume should not be null");
      checkState(
          webIdTokenProviderFQCN() != null,
          "The web id token provider fully qualified class name should not be null");
      setCredentialsProviderDelegate(
          credentialsProviderDelegate()
              .orElse(
                  CredentialsProviderDelegate.create(
                      Suppliers.memoize(() -> WebIdTokenProvider.create(webIdTokenProviderFQCN())),
                      audience(),
                      assumedRoleArn(),
                      sessionDurationSecs())));
      return autoBuild();
    }
  }

  /**
   * Given the {@link StsAssumeRoleWithWebIdentityCredentialsProvider} is final and can not be
   * easily mocked for testing purposes, this simple delegate container will be used to simplify
   * unit testing.
   */
  static class CredentialsProviderDelegate {

    private final Supplier<StsAssumeRoleWithWebIdentityCredentialsProvider>
        credentialsProviderDelegate;

    CredentialsProviderDelegate(
        Supplier<StsAssumeRoleWithWebIdentityCredentialsProvider> credentialsProviderDelegate) {
      this.credentialsProviderDelegate = credentialsProviderDelegate;
    }

    public static CredentialsProviderDelegate create(
        Supplier<WebIdTokenProvider> webIdTokenProvider,
        String audience,
        String assumedRoleArn,
        @Nullable Integer sessionDurationSecs) {
      return new CredentialsProviderDelegate(
          Suppliers.memoize(
              () ->
                  createCredentialsDelegate(
                      webIdTokenProvider, audience, assumedRoleArn, sessionDurationSecs)));
    }

    public AwsCredentials resolveCredentials() {
      return credentialsProviderDelegate.get().resolveCredentials();
    }

    public void close() {
      credentialsProviderDelegate.get().close();
    }

    static StsAssumeRoleWithWebIdentityCredentialsProvider createCredentialsDelegate(
        Supplier<WebIdTokenProvider> webIdTokenProvider,
        String audience,
        String assumedRoleArn,
        @Nullable Integer sessionDurationSecs) {
      return StsAssumeRoleWithWebIdentityCredentialsProvider.builder()
          .asyncCredentialUpdateEnabled(true)
          .refreshRequest(
              () ->
                  AssumeRoleWithWebIdentityRequest.builder()
                      .webIdentityToken(webIdTokenProvider.get().resolveTokenValue(audience))
                      .roleArn(assumedRoleArn)
                      .roleSessionName("beam-federated-session-" + UUID.randomUUID())
                      .durationSeconds(
                          Optional.ofNullable(sessionDurationSecs)
                              .orElse(DEFAULT_SESSION_DURATION_SECS))
                      .build())
          .stsClient(
              StsClient.builder()
                  .region(Region.AWS_GLOBAL)
                  .credentialsProvider(AnonymousCredentialsProvider.create())
                  .build())
          .build();
    }
  }
}
