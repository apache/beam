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
package org.apache.beam.it.neo4j;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.neo4j.Neo4jResourceManagerUtils.ILLEGAL_DATABASE_NAME_CHARS;

import org.junit.Test;

public class Neo4jResourceManagerUtilsTest {

  @Test
  public void excludesInvalidDatabaseNames() {
    assertThat(isValid("1db")).isFalse();
    assertThat(isValid(".db")).isFalse();
    assertThat(isValid("_db")).isFalse();
    assertThat(isValid(" db")).isFalse();
    assertThat(isValid("myDb.")).isFalse();
    assertThat(isValid("myDb-")).isFalse();
    assertThat(isValid("_internal")).isFalse();
    assertThat(isValid("system-of-a-db")).isFalse();
    assertThat(isValid("my_db")).isFalse();
  }

  @Test
  public void doesNotExcludeValidDatabaseNames() {
    assertThat(isValid("db1")).isTrue();
    assertThat(isValid("my-db")).isTrue();
    assertThat(isValid("a.database")).isTrue();
    assertThat(isValid("my-db2")).isTrue();
    assertThat(isValid("a.gr8.database")).isTrue();
  }

  private static boolean isValid(String name) {
    return !ILLEGAL_DATABASE_NAME_CHARS.matcher(name).find();
  }
}
