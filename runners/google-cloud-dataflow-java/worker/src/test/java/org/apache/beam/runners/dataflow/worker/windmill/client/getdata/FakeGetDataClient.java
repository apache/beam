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
package org.apache.beam.runners.dataflow.worker.windmill.client.getdata;

import java.io.PrintWriter;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;

/** Fake {@link GetDataClient} implementation for testing. */
public final class FakeGetDataClient implements GetDataClient {
  @Override
  public Windmill.KeyedGetDataResponse getStateData(
      String computationId, Windmill.KeyedGetDataRequest request) throws GetDataException {
    return Windmill.KeyedGetDataResponse.getDefaultInstance();
  }

  @Override
  public Windmill.GlobalData getSideInputData(Windmill.GlobalDataRequest request)
      throws GetDataException {
    return Windmill.GlobalData.getDefaultInstance();
  }

  @Override
  public void printHtml(PrintWriter writer) {}
}
