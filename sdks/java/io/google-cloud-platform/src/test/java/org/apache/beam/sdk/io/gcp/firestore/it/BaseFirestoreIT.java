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
package org.apache.beam.sdk.io.gcp.firestore.it;

import static org.apache.beam.sdk.io.gcp.firestore.it.FirestoreTestingHelper.assumeEnvVarSet;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assume.assumeThat;

import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreIOOptions;
import org.apache.beam.sdk.io.gcp.firestore.RpcQosOptions;
import org.apache.beam.sdk.io.gcp.firestore.it.FirestoreTestingHelper.CleanupMode;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.AssumptionViolatedException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;

@SuppressWarnings({"initialization.fields.uninitialized", "initialization.static.fields.uninitialized"}) // fields are managed via #beforeClass & #setup
abstract class BaseFirestoreIT {

  private static final String ENV_GOOGLE_APPLICATION_CREDENTIALS = "GOOGLE_APPLICATION_CREDENTIALS";
  private static final String ENV_FIRESTORE_EMULATOR_HOST = "FIRESTORE_EMULATOR_HOST";
  private static final String ENV_GOOGLE_CLOUD_PROJECT = "GOOGLE_CLOUD_PROJECT";

  @Rule(order = 1)
  public final TestName testName = new TestName();
  @Rule(order = 2)  // ensure our helper is "outer" to the timeout so we are allowed to cleanup even if a test times out
  public final FirestoreTestingHelper helper = new FirestoreTestingHelper(CleanupMode.ALWAYS);
  @Rule(order = 3)
  public final Timeout timeout = new Timeout(5, TimeUnit.MINUTES);
  @Rule(order = 4)
  public final TestPipeline testPipeline = TestPipeline.create();

  protected static String project;
  protected static RpcQosOptions rpcQosOptions;
  protected GcpOptions options;

  @BeforeClass
  public static void beforeClass() {
    try {
      assumeEnvVarSet(ENV_GOOGLE_APPLICATION_CREDENTIALS);
    } catch (AssumptionViolatedException e) {
      try {
        assumeEnvVarSet(ENV_FIRESTORE_EMULATOR_HOST);
      } catch (AssumptionViolatedException exception) {
        assumeThat(String.format("Either %s or %s must be set", ENV_GOOGLE_APPLICATION_CREDENTIALS, ENV_FIRESTORE_EMULATOR_HOST), false, equalTo(true));
      }
    }
    project = assumeEnvVarSet(ENV_GOOGLE_CLOUD_PROJECT);
    rpcQosOptions = RpcQosOptions.defaultOptions().toBuilder()
        .withMaxAttempts(1)
        .withHintMaxNumWorkers(1)
        .build();
  }

  @Before
  public void setup() {
    options = TestPipeline.testingPipelineOptions().as(GcpOptions.class);
    String emulatorHostPort = System.getenv(ENV_FIRESTORE_EMULATOR_HOST);
    if (emulatorHostPort != null) {
      options.as(FirestoreIOOptions.class).setEmulatorHostPort(emulatorHostPort);
    }
    options.setProject(project);
  }

}
