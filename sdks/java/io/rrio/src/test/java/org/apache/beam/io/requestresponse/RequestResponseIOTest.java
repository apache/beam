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

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link RequestResponseIO}. */
@RunWith(JUnit4.class)
public class RequestResponseIOTest {
  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void givenMinimalConfiguration_transformExpandsWithCallerOnly() {}

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
}
