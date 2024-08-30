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
package org.apache.beam.sdk.io.aws.s3;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

/**
 * Hamcrest {@link Matcher} to match {@link MatchResult}. Necessary because {@link
 * MatchResult#metadata()} throws an exception under normal circumstances.
 */
class MatchResultMatcher extends BaseMatcher<MatchResult> {

  private final MatchResult.Status expectedStatus;
  private final List<MatchResult.Metadata> expectedMetadata;
  private final IOException expectedException;

  private MatchResultMatcher(
      MatchResult.Status expectedStatus,
      List<MatchResult.Metadata> expectedMetadata,
      IOException expectedException) {
    this.expectedStatus = checkNotNull(expectedStatus);
    checkArgument((expectedMetadata == null) ^ (expectedException == null));
    this.expectedMetadata = expectedMetadata;
    this.expectedException = expectedException;
  }

  static MatchResultMatcher create(List<MatchResult.Metadata> expectedMetadata) {
    return new MatchResultMatcher(MatchResult.Status.OK, expectedMetadata, null);
  }

  private static MatchResultMatcher create(MatchResult.Metadata expectedMetadata) {
    return create(ImmutableList.of(expectedMetadata));
  }

  static MatchResultMatcher create(
      long sizeBytes, long lastModifiedMillis, ResourceId resourceId, boolean isReadSeekEfficient) {
    return create(
        MatchResult.Metadata.builder()
            .setSizeBytes(sizeBytes)
            .setLastModifiedMillis(lastModifiedMillis)
            .setResourceId(resourceId)
            .setIsReadSeekEfficient(isReadSeekEfficient)
            .build());
  }

  static MatchResultMatcher create(
      MatchResult.Status expectedStatus, IOException expectedException) {
    return new MatchResultMatcher(expectedStatus, null, expectedException);
  }

  static MatchResultMatcher create(MatchResult expected) {
    MatchResult.Status expectedStatus = expected.status();
    List<MatchResult.Metadata> expectedMetadata = null;
    IOException expectedException = null;
    try {
      expectedMetadata = expected.metadata();
    } catch (IOException e) {
      expectedException = e;
    }
    return new MatchResultMatcher(expectedStatus, expectedMetadata, expectedException);
  }

  @Override
  public boolean matches(Object actual) {
    if (actual == null) {
      return false;
    }
    if (!(actual instanceof MatchResult)) {
      return false;
    }
    MatchResult actualResult = (MatchResult) actual;
    if (!expectedStatus.equals(actualResult.status())) {
      return false;
    }

    List<MatchResult.Metadata> actualMetadata;
    try {
      actualMetadata = actualResult.metadata();
    } catch (IOException e) {
      return expectedException != null && expectedException.toString().equals(e.toString());
    }
    return expectedMetadata != null && expectedMetadata.equals(actualMetadata);
  }

  @Override
  public void describeTo(Description description) {
    if (expectedMetadata != null) {
      description.appendText(MatchResult.create(expectedStatus, expectedMetadata).toString());
    } else {
      description.appendText(MatchResult.create(expectedStatus, expectedException).toString());
    }
  }
}
