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
package org.apache.beam.sdk.io.fs;

import static org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions.RESOLVE_DIRECTORY;
import static org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions.RESOLVE_FILE;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.io.FileSystems;

/** A utility to test {@link ResourceId} implementations. */
@Experimental(Kind.FILESYSTEM)
public final class ResourceIdTester {
  /**
   * Enforces that the {@link ResourceId} implementation of {@code baseDirectory} meets the {@link
   * ResourceId} spec.
   */
  public static void runResourceIdBattery(ResourceId baseDirectory) {
    checkArgument(
        baseDirectory.isDirectory(), "baseDirectory %s is not a directory", baseDirectory);

    List<ResourceId> allResourceIds = new ArrayList<>();
    allResourceIds.add(baseDirectory);

    // Validate that individual resources meet the fairly restrictive spec we have.
    validateResourceIds(allResourceIds);

    // Validate operations with resolving child resources.
    validateResolvingIds(baseDirectory, allResourceIds);

    // Validate safeguards against resolving bad paths.
    validateFailureResolvingIds(baseDirectory);
  }

  private static void validateResolvingIds(
      ResourceId baseDirectory, List<ResourceId> allResourceIds) {
    ResourceId file1 = baseDirectory.resolve("child1", RESOLVE_FILE);
    ResourceId file2 = baseDirectory.resolve("child2", RESOLVE_FILE);
    ResourceId file2a = baseDirectory.resolve("child2", RESOLVE_FILE);
    allResourceIds.add(file1);
    allResourceIds.add(file2);
    assertThat("Resolved file isDirectory()", file1.isDirectory(), is(false));
    assertThat("Resolved file isDirectory()", file2.isDirectory(), is(false));
    assertThat("Resolved file isDirectory()", file2a.isDirectory(), is(false));

    ResourceId dir1 = baseDirectory.resolve("child1", RESOLVE_DIRECTORY);
    ResourceId dir2 = baseDirectory.resolve("child2", RESOLVE_DIRECTORY);
    ResourceId dir2a = baseDirectory.resolve("child2", RESOLVE_DIRECTORY);
    assertThat("Resolved directory isDirectory()", dir1.isDirectory(), is(true));
    assertThat("Resolved directory isDirectory()", dir2.isDirectory(), is(true));
    assertThat("Resolved directory isDirectory()", dir2a.isDirectory(), is(true));
    allResourceIds.add(dir1);
    allResourceIds.add(dir2);

    // ResourceIds in equality groups.
    assertEqualityGroups(
        Arrays.asList(
            Arrays.asList(file1),
            Arrays.asList(file2, file2a),
            Arrays.asList(dir1, dir1.getCurrentDirectory()),
            Arrays.asList(dir2, dir2a, dir2.getCurrentDirectory()),
            Arrays.asList(
                baseDirectory, file1.getCurrentDirectory(), file2.getCurrentDirectory())));

    // ResourceId toString() in equality groups.
    assertEqualityGroups(
        Arrays.asList(
            Arrays.asList(file1.toString()),
            Arrays.asList(file2.toString(), file2a.toString()),
            Arrays.asList(dir1.toString(), dir1.getCurrentDirectory().toString()),
            Arrays.asList(dir2.toString(), dir2a.toString(), dir2.getCurrentDirectory().toString()),
            Arrays.asList(
                baseDirectory.toString(),
                file1.getCurrentDirectory().toString(),
                file2.getCurrentDirectory().toString())));

    // TODO: test resolving strings that need to be escaped.
    //   Possible spec: https://tools.ietf.org/html/rfc3986#section-2
    //   May need options to be filesystem-independent, e.g., if filesystems ban certain chars.
  }

  /**
   * Asserts that all elements in each group are equal to each other but not equal to any other
   * element in another group.
   */
  private static <T> void assertEqualityGroups(List<List<T>> equalityGroups) {
    for (int i = 0; i < equalityGroups.size(); ++i) {
      List<T> current = equalityGroups.get(i);
      for (int j = 0; j < current.size(); ++j) {
        for (int k = 0; k < current.size(); ++k) {
          assertEquals(
              "Value at " + j + " should equal value at " + k + " in equality group " + i,
              current.get(j),
              current.get(k));
        }
      }
      for (int j = 0; j < equalityGroups.size(); ++j) {
        if (i == j) {
          continue;
        }
        assertTrue(
            current + " should not match any in " + equalityGroups.get(j),
            Collections.disjoint(current, equalityGroups.get(j)));
      }
    }
  }

  private static void validateFailureResolvingIds(ResourceId baseDirectory) {
    try {
      ResourceId badFile = baseDirectory.resolve("file/", RESOLVE_FILE);
      fail(String.format("Resolving badFile %s should have failed", badFile));
    } catch (IllegalArgumentException e) {
      // expected
    }

    ResourceId file = baseDirectory.resolve("file", RESOLVE_FILE);
    try {
      file.resolve("file2", RESOLVE_FILE);
      fail(String.format("Should not be able to resolve against file resource %s", file));
    } catch (IllegalStateException e) {
      // expected
    }
  }

  private static void validateResourceIds(List<ResourceId> resourceIds) {
    for (ResourceId resourceId : resourceIds) {
      // ResourceIds should equal themselves.
      assertThat("ResourceId equal to itself", resourceId, equalTo(resourceId));

      // ResourceIds should be clonable via FileSystems#matchNewResource.
      ResourceId cloned;
      if (resourceId.isDirectory()) {
        cloned = FileSystems.matchNewResource(resourceId.toString(), true /* isDirectory */);
      } else {
        cloned = FileSystems.matchNewResource(resourceId.toString(), false /* isDirectory */);
      }
      assertThat("ResourceId equals clone of itself", cloned, equalTo(resourceId));
      // .. and clones have consistent toString.
      assertThat(
          "ResourceId toString consistency", cloned.toString(), equalTo(resourceId.toString()));
      // .. and have consistent isDirectory.
      assertThat(
          "ResourceId isDirectory consistency",
          cloned.isDirectory(),
          equalTo(resourceId.isDirectory()));
    }
  }

  private ResourceIdTester() {} // prevent instantiation
}
