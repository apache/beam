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
package org.apache.beam.sdk.io.gcp.bigquery;

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

import java.util.Set;
import java.util.regex.Pattern;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;

/** Constants and variables for CDC support. */
public class StorageApiCDC {
  public static final String CHANGE_SQN_COLUMN = "_CHANGE_SEQUENCE_NUMBER";
  public static final String CHANGE_TYPE_COLUMN = "_CHANGE_TYPE";
  public static final Set<String> COLUMNS = ImmutableSet.of(CHANGE_TYPE_COLUMN, CHANGE_SQN_COLUMN);

  /**
   * Expected valid pattern for a {@link #CHANGE_SQN_COLUMN} value for use with BigQuery's {@code
   * _CHANGE_SEQUENCE_NUMBER} format. See {@link
   * RowMutationInformation#of(RowMutationInformation.MutationType, String)} for more details.
   */
  public static final Pattern EXPECTED_SQN_PATTERN =
      Pattern.compile("^([0-9A-Fa-f]{1,16})(/([0-9A-Fa-f]{1,16})){0,3}$");
}
