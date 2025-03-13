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
package org.apache.beam.sdk.io.aws2;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.WeakHashMap;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.aws2.common.ClientBuilderFactory;
import org.apache.beam.sdk.io.aws2.common.ClientConfiguration;
import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;

/**
 * A mock {@link ClientBuilderFactory} to facilitate unit tests with mocked AWS clients. This
 * factory returns a mocked builder with a preconfigured client which is tied to a specific instance
 * of a pipeline or pipeline options respectively (using {@link #set(TestPipeline, Class, Object)}
 * or {@link #set(PipelineOptions, Class, Object)}), if available. Otherwise and empty mock is
 * returned.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * public final TestPipeline pipeline = TestPipeline.create();
 * public final S3Client s3 = Mockito.mock(S3Client.class)
 *
 * @Before
 * public void configureClientBuilderFactory() {
 *  StaticClientBuilderFactory.set(pipeline, S3ClientBuilder.class, s3);
 * }
 * }</pre>
 */
public class MockClientBuilderFactory implements ClientBuilderFactory {
  private static final Logger LOG = LoggerFactory.getLogger(MockClientBuilderFactory.class);

  @SuppressWarnings("rawtypes")
  private static final WeakHashMap<
          PipelineOptions, Map<Class<? extends AwsClientBuilder>, AwsClientBuilder>>
      CLIENTS = new WeakHashMap<>();

  public static <BuilderT extends AwsClientBuilder<BuilderT, ClientT>, ClientT> void set(
      TestPipeline pipeline, Class<BuilderT> builderClass, ClientT clientT) {
    set(pipeline.getOptions(), builderClass, clientT);
  }

  public static <BuilderT extends AwsClientBuilder<BuilderT, ClientT>, ClientT> void set(
      PipelineOptions options, Class<BuilderT> builderClass, ClientT clientT) {
    options.as(AwsOptions.class).setClientBuilderFactory(MockClientBuilderFactory.class);

    BuilderT builder = mock(builderClass);
    when(builder.build()).thenReturn(clientT);
    CLIENTS.computeIfAbsent(options, ignore -> new HashMap<>()).put(builderClass, builder);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public <BuilderT extends AwsClientBuilder<BuilderT, ClientT>, ClientT> BuilderT create(
      BuilderT builder, ClientConfiguration clientConfig, @Nullable AwsOptions options) {
    // safe to cast: builder is instance of key (builder class) and value is mocked accordingly
    Optional<Map.Entry<Class<? extends AwsClientBuilder>, AwsClientBuilder>> mock =
        CLIENTS.entrySet().stream()
            .filter(kv -> kv.getKey().getOptionsId() == options.getOptionsId())
            .flatMap(kv -> kv.getValue().entrySet().stream())
            .filter(b -> b.getKey().isInstance(builder))
            .findFirst();

    if (mock.isPresent()) {
      return (BuilderT) mock.get().getValue();
    } else {
      Class<BuilderT> builderType = (Class<BuilderT>) builder.getClass().getInterfaces()[0];
      LOG.warn("No mock configured for {}, returning empty mock.", builderType.getSimpleName());
      return mock(builderType);
    }
  }

  @Override
  public void checkConfiguration(ClientConfiguration clientConfig, @Nullable AwsOptions options) {}
}
