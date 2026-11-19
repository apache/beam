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
package org.apache.beam.sdk.io.delta;

import java.util.Map;

/**
 * Checks that Change Data Feed was enabled on a Delta Lake table for the versions that a change
 * read covers.
 *
 * <p>Delta Lake writers only record the changes of a commit while the table property {@code
 * delta.enableChangeDataFeed} is {@code true}. A commit made without it holds no change data files,
 * and reading its data files as inserts would silently lose its updates and deletes. Like Spark, a
 * change read therefore requires Change Data Feed to be enabled at its start version, and checks
 * that no change of the table configuration within its range disabled it.
 */
final class DeltaChangeDataFeedUtils {
  /** The Delta Lake table property that makes writers record the changes of each commit. */
  static final String ENABLE_CHANGE_DATA_FEED_PROPERTY = "delta.enableChangeDataFeed";

  private DeltaChangeDataFeedUtils() {}

  /**
   * Fails unless Change Data Feed was enabled at {@code version}, given the table properties at
   * that version, for a read of the versions {@code [startVersion, endVersion]}.
   *
   * @throws IllegalArgumentException if Change Data Feed was not enabled at {@code version}
   */
  static void checkEnabled(
      String tablePath,
      long version,
      Map<String, String> tableProperties,
      long startVersion,
      long endVersion) {
    if (!Boolean.parseBoolean(tableProperties.get(ENABLE_CHANGE_DATA_FEED_PROPERTY))) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot read the changes of Delta Lake table '%s' in versions [%d, %d]: Change Data"
                  + " Feed was not enabled at version %d, so the changes of that version were not"
                  + " recorded. Read changes starting from a version at or after the one that set"
                  + " the table property '%s' = 'true' (see the history of the table), or set it"
                  + " with ALTER TABLE ... SET TBLPROPERTIES ('%s' = 'true') to start recording"
                  + " changes.",
              tablePath,
              startVersion,
              endVersion,
              version,
              ENABLE_CHANGE_DATA_FEED_PROPERTY,
              ENABLE_CHANGE_DATA_FEED_PROPERTY));
    }
  }
}
