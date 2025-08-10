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
package org.apache.beam.sdk.testing;

/**
 * This class moved to the module sdks/java/testing/junit (artifact
 * org.apache.beam:beam-sdks-java-testing-junit). Include that module in your tests and import
 * org.apache.beam.sdk.testing.TestPipelineExtension from there.
 *
 * <p>This stub remains only to avoid compile errors if referenced from production code; it is not a
 * usable JUnit 5 extension in this module.
 *
 * @deprecated Use {@code org.apache.beam:beam-sdks-java-testing-junit} and import {@code
 *     org.apache.beam.sdk.testing.TestPipelineExtension} from that module.
 */
@Deprecated
public final class TestPipelineExtension {
  private TestPipelineExtension() {
    throw new UnsupportedOperationException(
        "TestPipelineExtension moved to sdks/java/testing/junit. Add dependency on "
            + "beam-sdks-java-testing-junit in your test scope.");
  }
}
