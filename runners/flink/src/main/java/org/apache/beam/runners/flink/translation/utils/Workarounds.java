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
package org.apache.beam.runners.flink.translation.utils;

import com.fasterxml.jackson.databind.type.TypeFactory;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.program.OptimizerPlanEnvironment;

/** Workarounds for dealing with limitations of Flink or its libraries. */
public class Workarounds {

  public static void deleteStaticCaches() {
    // Clear cache to get rid of any references to the Flink Classloader
    // See https://jira.apache.org/jira/browse/BEAM-6460
    TypeFactory.defaultInstance().clearCache();
  }

  /**
   * Flink uses the {@link org.apache.flink.client.program.OptimizerPlanEnvironment} which replaces
   * stdout/stderr during job graph creation. This was intended only for previewing the plan, but
   * other parts of Flink, e.g. the Rest API have started to use this code as well. To be able to
   * inspect the output before execution, we use this method to restore the original stdout/stderr.
   */
  public static void restoreOriginalStdOutAndStdErrIfApplicable() {
    if (ExecutionEnvironment.getExecutionEnvironment() instanceof OptimizerPlanEnvironment) {
      System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
      System.setErr(new PrintStream(new FileOutputStream(FileDescriptor.err)));
    }
  }
}
