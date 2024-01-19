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
package org.apache.beam.it.cassandra;

import static org.apache.beam.it.common.utils.ResourceManagerUtils.generateResourceId;

import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;

/** Utilities for {@link CassandraResourceManager} implementations. */
final class CassandraResourceManagerUtils {

  private static final int MAX_DATABASE_NAME_LENGTH = 63;
  private static final Pattern ILLEGAL_DATABASE_NAME_CHARS =
      Pattern.compile("[/\\\\. \"\0$]"); // i.e. [/\. "$]
  private static final String REPLACE_DATABASE_NAME_CHAR = "-";
  private static final DateTimeFormatter TIME_FORMAT =
      DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss-SSSSSS");

  private CassandraResourceManagerUtils() {}

  /**
   * Generates a Cassandra keyspace name from a given string.
   *
   * @param baseString The string to generate the name from.
   * @return The keyspace name string.
   */
  static String generateKeyspaceName(String baseString) {
    return generateResourceId(
            baseString,
            ILLEGAL_DATABASE_NAME_CHARS,
            REPLACE_DATABASE_NAME_CHAR,
            MAX_DATABASE_NAME_LENGTH,
            TIME_FORMAT)
        .replace('-', '_');
  }
}
