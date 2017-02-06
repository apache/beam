/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.beam.sdk.io.hadoop.inputformat.testing;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import javax.annotation.Nonnull;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.SerializableMatcher;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

/**
 * A matcher class to verify if the checksum of expected list of Strings matches with the checksum
 * of a file content.
 */
public class HIFIOTextMatcher extends TypeSafeMatcher<PipelineResult> implements
    SerializableMatcher<PipelineResult> {

  private static final long serialVersionUID = 1L;
  private static final Logger LOGGER = LoggerFactory.getLogger(HIFIOTextMatcher.class);
  private String expectedChecksum;
  private String actualChecksum;
  private String filePath;
  private List<String> expectedList;

  public HIFIOTextMatcher(String filePath, List<String> expectedList) {
    checkArgument(!Strings.isNullOrEmpty(filePath), "Expected valid file path, but received %s",
        filePath);
    checkArgument(null != expectedList && !expectedList.isEmpty(),
        "Expected a valid list, but received %s", expectedList);
    this.filePath = filePath;
    this.expectedList = expectedList;
  }

  public HIFIOTextMatcher(String filePath, String expectedChecksum) {
    checkArgument(!Strings.isNullOrEmpty(filePath), "Expected valid file path, but received %s",
        filePath);
    checkArgument(!Strings.isNullOrEmpty(filePath),
        "Expected a valid checksum value, but received %s", expectedChecksum);
    this.filePath = filePath;
    this.expectedChecksum = expectedChecksum;
  }

  @Override
  public void describeTo(Description description) {
    description
    .appendText("Expected checksum is (")
    .appendText(expectedChecksum)
    .appendText(")");
  }

  /**
   * This method checks if the checksum of provided expected list of Strings/ provided expected
   * checksum matches with the checksum of file content.
   */
  @Override
  protected boolean matchesSafely(PipelineResult arg0) {
    if (Strings.isNullOrEmpty(expectedChecksum)) {
      expectedChecksum = generateHash(expectedList);
    }
    ArrayList<String> actualList = new ArrayList<String>();
    Scanner s;
    try {
      s = new Scanner(new File(filePath));
      while (s.hasNext()) {
        actualList.add(s.next());
      }
      s.close();
    } catch (FileNotFoundException e) {
      LOGGER.warn(e.getMessage());
    }
    actualChecksum = generateHash(actualList);
    return expectedChecksum.equals(actualChecksum);
  }

  /**
   * This method generates checksum for a list of Strings.
   * @param elements
   * @return
   */
  private String generateHash(@Nonnull List<String> elements) {
    List<HashCode> rowHashes = Lists.newArrayList();
    for (String element : elements) {
      rowHashes.add(Hashing.sha1().hashString(element, StandardCharsets.UTF_8));
    }
    return Hashing.combineUnordered(rowHashes).toString();
  }

}
