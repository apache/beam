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
package org.apache.beam.sdk.extensions.sql.jdbc;

import static org.apache.beam.sdk.extensions.sql.impl.JdbcDriver.CONNECT_STRING_PREFIX;

import java.io.IOException;
import sqlline.SqlLine;

/** {@link BeamSqlLine} provides default arguments to SqlLine. */
public class BeamSqlLine {
  public static void main(String[] args) throws IOException {
    String[] args2 = new String[2 + args.length];
    args2[0] = "-u";
    args2[1] = CONNECT_STRING_PREFIX;
    System.arraycopy(args, 0, args2, 2, args.length);

    SqlLine.main(args2);
  }
}
