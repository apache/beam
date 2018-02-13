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

package org.apache.beam.runners.core.construction;

import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Utilities for dealing with URNs. */
public class UrnUtils {

  private static final String STANDARD_URNS_PATH = "org/apache/beam/model/common_urns.md";
  private static final Pattern URN_REGEX = Pattern.compile("\\b(urn:)?beam:\\S+:v[0-9.]+");
  private static final Set<String> COMMON_URNS = extractUrnsFromPath(STANDARD_URNS_PATH);

  private static Set<String> extractUrnsFromPath(String path) {
    String contents;
    try {
      contents = CharStreams.toString(new InputStreamReader(
          UrnUtils.class.getClassLoader().getResourceAsStream(path)));
    } catch (IOException exn) {
      throw new RuntimeException(exn);
    }
    Set<String> urns = new HashSet<>();
    Matcher m = URN_REGEX.matcher(contents);
    while (m.find()) {
      urns.add(m.group());
    }
    return urns;
  }

  public static String validateCommonUrn(String urn) {
    if (!URN_REGEX.matcher(urn).matches()) {
      throw new IllegalArgumentException(
          String.format("'%s' does not match '%s'", urn, URN_REGEX));
    }
    if (!COMMON_URNS.contains(urn)) {
      throw new IllegalArgumentException(
          String.format("'%s' is not found in '%s'", urn, STANDARD_URNS_PATH));
    }
    return urn;
  }
}
