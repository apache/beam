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
package org.apache.beam.sdk.io.cdap.context;

import static org.junit.Assert.assertTrue;

import io.cdap.cdap.etl.api.streaming.StreamingSourceContext;
import java.sql.Timestamp;
import org.junit.Test;

/** Test class for {@link StreamingSourceContextImpl}. */
public class StreamingSourceContextImplTest {

  /**
   * TODO: Remove tests(getFailureCollector, getLogicalStartTime) if these methods weren't override
   * and were implemented own methods of StreamingSourceContextImplTest class.
   */
  @Test
  public void getLogicalStartTime() {
    /** arrange */
    StreamingSourceContext context = new StreamingSourceContextImpl();
    Timestamp startTime = new Timestamp(System.currentTimeMillis());

    /** assert */
    assertTrue(startTime.getTime() - context.getLogicalStartTime() <= 100);
  }
}
