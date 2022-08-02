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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.ValidationException;
import java.sql.Timestamp;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link BatchContextImpl}. */
@RunWith(JUnit4.class)
public class BatchContextImplTest {

  @Test
  public void getLogicalStartTime() {
    /** arrange */
    Timestamp expectedStartTime = new Timestamp(System.currentTimeMillis());
    BatchContextImpl context = new BatchSourceContextImpl();

    /** act */
    long actualStartTime = context.getLogicalStartTime();

    /** assert */
    assertTrue((expectedStartTime.getTime() - actualStartTime) <= 100);
  }

  @Test
  public void getFailureCollector() {
    /** arrange */
    BatchContextImpl context = new BatchSinkContextImpl();

    /** act */
    FailureCollector failureCollector = context.getFailureCollector();

    /** assert */
    ValidationException validationException = failureCollector.getOrThrowException();
    assertEquals(0, validationException.getFailures().size());
  }
}
