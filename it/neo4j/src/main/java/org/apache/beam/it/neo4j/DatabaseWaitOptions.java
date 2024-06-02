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

public class DatabaseWaitOptions {

  public static DatabaseWaitOption waitDatabase() {
    return DatabaseWait.WAIT;
  }

  public static DatabaseWaitOption waitDatabase(int seconds) {
    return new DatabaseWaitInSeconds(seconds);
  }

  public static DatabaseWaitOption noWaitDatabase() {
    return DatabaseNoWait.NO_WAIT;
  }

  static String asCypher(DatabaseWaitOption option) {
    if (option == null || option == DatabaseNoWait.NO_WAIT) {
      return "NOWAIT";
    }
    if (option == DatabaseWait.WAIT) {
      return "WAIT";
    }
    if (option instanceof DatabaseWaitInSeconds) {
      DatabaseWaitInSeconds wait = (DatabaseWaitInSeconds) option;
      return String.format("WAIT %s SECONDS", wait.getSeconds());
    }
    throw new Neo4jResourceManagerException(
        String.format("Unsupported wait option type %s", option.getClass()));
  }

  private enum DatabaseNoWait implements DatabaseWaitOption {
    NO_WAIT;
  }

  private enum DatabaseWait implements DatabaseWaitOption {
    WAIT;
  }

  private static class DatabaseWaitInSeconds implements DatabaseWaitOption {
    private final int seconds;

    public DatabaseWaitInSeconds(int seconds) {
      this.seconds = seconds;
    }

    public int getSeconds() {
      return seconds;
    }
  }
}
