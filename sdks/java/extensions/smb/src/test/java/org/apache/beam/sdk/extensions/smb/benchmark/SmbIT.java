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
package org.apache.beam.sdk.extensions.smb.benchmark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.beam.sdk.coders.CannotProvideCoderException;

/** Integration. */
public class SmbIT {
  public static void main(String[] args) throws CannotProvideCoderException, IOException {
    final Path temp = Files.createTempDirectory("smb-");

    String[] sinkArgs = new String[args.length + 3];
    sinkArgs[0] = "--avroDestination=" + temp.resolve("avro");
    sinkArgs[1] = "--jsonDestination=" + temp.resolve("json");
    sinkArgs[2] = "--tempLocation=" + temp.resolve("temp");

    System.arraycopy(args, 0, sinkArgs, 3, args.length);

    SinkBenchmark.main(sinkArgs);

    SourceBenchmark.main(
        new String[] {
          "--avroSource=" + temp.resolve("avro"),
          "--jsonSource=" + temp.resolve("json"),
          "--tempLocation=" + temp.resolve("temp"),
        });
  }
}
