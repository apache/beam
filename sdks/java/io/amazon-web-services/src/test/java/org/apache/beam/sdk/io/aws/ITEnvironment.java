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
package org.apache.beam.sdk.io.aws;

import static org.apache.beam.sdk.testing.TestPipeline.testingPipelineOptions;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import org.apache.beam.sdk.io.aws.options.AwsOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.junit.rules.ExternalResource;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

/**
 * JUnit rule providing an integration testing environment for AWS as {@link ExternalResource}.
 *
 * <p>This rule is typically used as @ClassRule. It starts a Localstack container with the requested
 * AWS service and provides matching {@link AwsOptions}. The usage of localstack can also be
 * disabled using {@link ITOptions} pipeline options to run integration tests against AWS, for
 * instance:
 *
 * <pre>{@code
 * ./gradlew :sdks:java:io:amazon-web-services:integrationTest \
 *   --info \
 *   --tests "org.apache.beam.sdk.io.aws.s3.S3FileSystemIT" \
 *   -DintegrationTestPipelineOptions='["--awsRegion=eu-central-1","--useLocalstack=false"]'
 * }</pre>
 *
 * @param <OptionsT> The options type to use for the integration test.
 */
public class ITEnvironment<OptionsT extends ITEnvironment.ITOptions> extends ExternalResource {
  private static final String LOCALSTACK = "localstack/localstack";
  private static final String LOCALSTACK_VERSION = "0.13.1";

  public interface ITOptions extends AwsOptions, TestPipelineOptions {
    @Description("Number of rows to write and read by the test")
    @Default.Integer(1000)
    Integer getNumberOfRows();

    void setNumberOfRows(Integer count);

    @Description("Flag if to use localstack, enabled by default.")
    @Default.Boolean(true)
    Boolean getUseLocalstack();

    void setUseLocalstack(Boolean useLocalstack);

    @Description("Localstack log level, e.g. trace, debug, info")
    String getLocalstackLogLevel();

    void setLocalstackLogLevel(String level);
  }

  private final OptionsT options;
  private final LocalStackContainer localstack;

  public ITEnvironment(Service service, Class<OptionsT> optionsClass, String... env) {
    this(new Service[] {service}, optionsClass, env);
  }

  public ITEnvironment(Service[] services, Class<OptionsT> optionsClass, String... env) {
    localstack =
        new LocalStackContainer(DockerImageName.parse(LOCALSTACK).withTag(LOCALSTACK_VERSION))
            .withServices(services)
            .withStartupAttempts(3);

    PipelineOptionsFactory.register(optionsClass);
    options = testingPipelineOptions().as(optionsClass);

    localstack.setEnv(ImmutableList.copyOf(env));
    if (options.getLocalstackLogLevel() != null) {
      localstack
          .withEnv("LS_LOG", options.getLocalstackLogLevel())
          .withLogConsumer(
              new Slf4jLogConsumer(LoggerFactory.getLogger(StringUtils.join(services))));
    }
  }

  public TestPipeline createTestPipeline() {
    return TestPipeline.fromOptions(options);
  }

  public <BuilderT extends AwsClientBuilder<BuilderT, ClientT>, ClientT> ClientT buildClient(
      BuilderT builder) {
    if (options.getAwsServiceEndpoint() != null) {
      builder.withEndpointConfiguration(
          new EndpointConfiguration(options.getAwsServiceEndpoint(), options.getAwsRegion()));
    } else {
      builder.setRegion(options.getAwsRegion());
    }
    return builder.withCredentials(options.getAwsCredentialsProvider()).build();
  }

  public OptionsT options() {
    return options;
  }

  @Override
  protected void before() {
    if (options.getUseLocalstack()) {
      startLocalstack();
    }
  }

  @Override
  protected void after() {
    localstack.stop(); // noop if not started
  }

  /** Necessary setup for localstack environment. */
  private void startLocalstack() {
    localstack.start();
    options.setAwsServiceEndpoint(
        localstack.getEndpointOverride(S3).toString()); // service irrelevant
    options.setAwsRegion(localstack.getRegion());
    options.setAwsCredentialsProvider(
        new AWSStaticCredentialsProvider(
            new BasicAWSCredentials(localstack.getAccessKey(), localstack.getSecretKey())));
  }
}
