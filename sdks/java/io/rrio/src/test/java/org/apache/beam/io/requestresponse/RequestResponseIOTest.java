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
package org.apache.beam.io.requestresponse;

import static org.apache.beam.io.requestresponse.Cache.CACHE_READ_USING_REDIS;
import static org.apache.beam.io.requestresponse.Cache.CACHE_WRITE_USING_REDIS;
import static org.apache.beam.io.requestresponse.Call.NOOP_SETUP_TEARDOWN;
import static org.apache.beam.io.requestresponse.RequestResponseIO.CACHE_READ_NAME;
import static org.apache.beam.io.requestresponse.RequestResponseIO.CACHE_WRITE_NAME;
import static org.apache.beam.io.requestresponse.RequestResponseIO.CALL_NAME;
import static org.apache.beam.io.requestresponse.RequestResponseIO.ROOT_NAME;
import static org.apache.beam.io.requestresponse.RequestResponseIO.THROTTLE_NAME;
import static org.apache.beam.io.requestresponse.RequestResponseIO.WRAPPED_CALLER;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.google.auto.value.AutoValue;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaProvider;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link RequestResponseIO}. */
@RunWith(JUnit4.class)
public class RequestResponseIOTest {
  @Rule
  public final transient TestPipeline pipeline =
      TestPipeline.create().enableAbandonedNodeEnforcement(false);

  private static final TypeDescriptor<Request> REQUEST_TYPE = TypeDescriptor.of(Request.class);
  private static final TypeDescriptor<Response> RESPONSE_TYPE = TypeDescriptor.of(Response.class);
  private static final SchemaProvider SCHEMA_PROVIDER = new AutoValueSchema();
  private static final String THROTTLE_USING_EXTERNAL =
      ThrottleWithExternalResource.class.getName();

  private static final Coder<Request> REQUEST_CODER =
      SchemaCoder.of(
          checkStateNotNull(SCHEMA_PROVIDER.schemaFor(REQUEST_TYPE)),
          REQUEST_TYPE,
          checkStateNotNull(SCHEMA_PROVIDER.toRowFunction(REQUEST_TYPE)),
          checkStateNotNull(SCHEMA_PROVIDER.fromRowFunction(REQUEST_TYPE)));

  private static final Coder<Response> RESPONSE_CODER =
      SchemaCoder.of(
          checkStateNotNull(SCHEMA_PROVIDER.schemaFor(RESPONSE_TYPE)),
          RESPONSE_TYPE,
          checkStateNotNull(SCHEMA_PROVIDER.toRowFunction(RESPONSE_TYPE)),
          checkStateNotNull(SCHEMA_PROVIDER.fromRowFunction(RESPONSE_TYPE)));

  @Test
  public void givenMinimalConfiguration_transformExpandsWithCallOnly() {
    ExpansionPipelineVisitor visitor = new ExpansionPipelineVisitor();
    requests().apply(RequestResponseIO.of(new CallerImpl(), RESPONSE_CODER));
    pipeline.traverseTopologically(visitor);
    visitor.assertExpandsWithCallOf(CALL_NAME, CallerImpl.class.getName(), NOOP_SETUP_TEARDOWN);
    visitor.assertNotExpandsWith(THROTTLE_NAME);
    visitor.assertNotExpandsWith(CACHE_READ_NAME);
    visitor.assertNotExpandsWith(CACHE_WRITE_NAME);
  }

  @Test
  public void givenCallerAndSetupTeardown_transformExpandsWithCallOnly() {
    ExpansionPipelineVisitor visitor = new ExpansionPipelineVisitor();
    requests()
        .apply(
            RequestResponseIO.ofCallerAndSetupTeardown(
                new CallerSetupTeardownImpl(), RESPONSE_CODER));

    pipeline.traverseTopologically(visitor);

    visitor.assertExpandsWithCallOf(
        CALL_NAME,
        CallerSetupTeardownImpl.class.getName(),
        CallerSetupTeardownImpl.class.getName());

    visitor.assertNotExpandsWith(THROTTLE_NAME);
    visitor.assertNotExpandsWith(CACHE_READ_NAME);
    visitor.assertNotExpandsWith(CACHE_WRITE_NAME);
  }

  @Test
  public void givenPreventiveThrottlingUsingRedis_transformExpandsWithCallAndThrottling()
      throws NonDeterministicException {
    ExpansionPipelineVisitor visitor = new ExpansionPipelineVisitor();
    requests()
        .apply(
            RequestResponseIO.of(new CallerImpl(), RESPONSE_CODER)
                .withPreventiveThrottleUsingRedis(
                    URI.create("redis://localhost:6379"),
                    REQUEST_CODER,
                    new Quota(1L, Duration.standardSeconds(1L)),
                    "quota",
                    "queue"));

    pipeline.traverseTopologically(visitor);

    visitor.assertExpandsWithCallOf(CALL_NAME, CallerImpl.class.getName(), NOOP_SETUP_TEARDOWN);
    visitor.assertExpandsWithThrottleOf(THROTTLE_USING_EXTERNAL);
    visitor.assertNotExpandsWith(CACHE_READ_NAME);
    visitor.assertNotExpandsWith(CACHE_WRITE_NAME);
  }

  @Test
  public void givenCacheUsingRedis_transformExpandsWithCallAndCache()
      throws NonDeterministicException {
    ExpansionPipelineVisitor visitor = new ExpansionPipelineVisitor();
    requests()
        .apply(
            RequestResponseIO.of(new CallerImpl(), RESPONSE_CODER)
                .withRedisCache(
                    URI.create("redis://localhost:6379"),
                    REQUEST_CODER,
                    Duration.standardHours(1L)));

    pipeline.traverseTopologically(visitor);

    visitor.assertExpandsWithCallOf(CALL_NAME, WRAPPED_CALLER, NOOP_SETUP_TEARDOWN);
    visitor.assertNotExpandsWith(THROTTLE_NAME);
    visitor.assertExpandsWithCallOf(
        CACHE_READ_NAME, CACHE_READ_USING_REDIS, CACHE_READ_USING_REDIS);
    visitor.assertExpandsWithCallOf(
        CACHE_WRITE_NAME, CACHE_WRITE_USING_REDIS, CACHE_WRITE_USING_REDIS);
  }

  @Test
  public void givenAllConfigurationUsingRedis_transformExpandsWithEverything()
      throws NonDeterministicException {
    ExpansionPipelineVisitor visitor = new ExpansionPipelineVisitor();
    requests()
        .apply(
            RequestResponseIO.of(new CallerImpl(), RESPONSE_CODER)
                .withRedisCache(
                    URI.create("redis://localhost:6379"), REQUEST_CODER, Duration.standardHours(1L))
                .withPreventiveThrottleUsingRedis(
                    URI.create("redis://localhost:6379"),
                    REQUEST_CODER,
                    new Quota(1L, Duration.standardSeconds(1L)),
                    "quota",
                    "queue"));

    pipeline.traverseTopologically(visitor);

    visitor.assertExpandsWithCallOf(CALL_NAME, WRAPPED_CALLER, NOOP_SETUP_TEARDOWN);
    visitor.assertExpandsWithThrottleOf(THROTTLE_USING_EXTERNAL);
    visitor.assertExpandsWithCallOf(
        CACHE_READ_NAME, CACHE_READ_USING_REDIS, CACHE_READ_USING_REDIS);
    visitor.assertExpandsWithCallOf(
        CACHE_WRITE_NAME, CACHE_WRITE_USING_REDIS, CACHE_WRITE_USING_REDIS);
  }

  private static class ExpansionPipelineVisitor implements PipelineVisitor {

    private final Map<String, TransformHierarchy.Node> visits = new HashMap<>();

    private void assertExpandsWithCallOf(
        String stepName, String callerClassName, String setupTeardownClassName) {
      stepName = ROOT_NAME + "/" + stepName;
      PTransform<?, ?> transform = getFromStep(stepName);
      assertThat(transform.getClass(), equalTo(Call.class));
      Call<?, ?> call = (Call<?, ?>) transform;
      assertThat(
          call.getConfiguration().getCaller().getClass().getName(), equalTo(callerClassName));
      assertThat(
          call.getConfiguration().getSetupTeardown().getClass().getName(),
          equalTo(setupTeardownClassName));
    }

    private void assertExpandsWithThrottleOf(String className) {
      assertExpandsPTransformClassOf(ROOT_NAME + "/" + THROTTLE_NAME, className);
    }

    private void assertNotExpandsWith(String stepName) {
      stepName = ROOT_NAME + "/" + stepName;
      assertThat(visits.containsKey(stepName), equalTo(false));
    }

    private void assertExpandsPTransformClassOf(String stepName, String className) {
      PTransform<?, ?> transform = getFromStep(stepName);
      assertThat(transform.getClass().getName(), equalTo(className));
    }

    private @NonNull PTransform<?, ?> getFromStep(String name) {
      TransformHierarchy.Node node = checkStateNotNull(visits.get(name));
      return checkStateNotNull(node.getTransform());
    }

    @Override
    public void enterPipeline(Pipeline p) {}

    @Override
    public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
      visit(node);
      return CompositeBehavior.ENTER_TRANSFORM;
    }

    private void visit(TransformHierarchy.Node node) {
      visits.put(node.getFullName(), node);
    }

    @Override
    public void leaveCompositeTransform(TransformHierarchy.Node node) {}

    @Override
    public void visitPrimitiveTransform(TransformHierarchy.Node node) {}

    @Override
    public void visitValue(PValue value, TransformHierarchy.Node producer) {}

    @Override
    public void leavePipeline(Pipeline pipeline) {}
  }

  private PCollection<Request> requests() {
    return pipeline.apply(
        "requests", Create.of(requestOf("a", 1L), requestOf("b", 2L), requestOf("c", 3L)));
  }

  private static Request requestOf(String aString, Long aLong) {
    return Request.builder().setAString(aString).setALong(aLong).build();
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class Request {

    static Builder builder() {
      return new AutoValue_RequestResponseIOTest_Request.Builder();
    }

    abstract String getAString();

    abstract Long getALong();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setAString(String value);

      abstract Builder setALong(Long value);

      abstract Request build();
    }
  }

  @AutoValue
  abstract static class Response {
    static Builder builder() {
      return new AutoValue_RequestResponseIOTest_Response.Builder();
    }

    abstract String getAString();

    abstract Long getALong();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setAString(String value);

      abstract Builder setALong(Long value);

      abstract Response build();
    }
  }

  private static class CallerSetupTeardownImpl implements Caller<Request, Response>, SetupTeardown {
    private final CallerImpl caller = new CallerImpl();

    @Override
    public Response call(Request request) throws UserCodeExecutionException {
      return caller.call(request);
    }

    @Override
    public void setup() throws UserCodeExecutionException {}

    @Override
    public void teardown() throws UserCodeExecutionException {}
  }

  private static class CallerImpl implements Caller<Request, Response> {

    @Override
    public Response call(Request request) throws UserCodeExecutionException {
      return Response.builder()
          .setAString(request.getAString())
          .setALong(request.getALong())
          .build();
    }
  }
}
