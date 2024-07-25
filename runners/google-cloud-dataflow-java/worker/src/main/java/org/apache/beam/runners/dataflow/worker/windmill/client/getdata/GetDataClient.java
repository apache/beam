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
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalData;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataResponse;
import org.apache.beam.sdk.annotations.Internal;

/** Client for streaming backend GetData API. */
@Internal
public interface GetDataClient {
  /**
   * Issues a blocking call to fetch state data for a specific computation and {@link
   * org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem}.
   *
   * @throws GetDataException when there was an unexpected error during the attempted fetch.
   */
  KeyedGetDataResponse getStateData(String computationId, KeyedGetDataRequest request)
      throws GetDataException;

  GlobalData getSideInputData(GlobalDataRequest request) throws GetDataException;

  void printHtml(PrintWriter writer);

  final class GetDataException extends RuntimeException {
    GetDataException(String message, Throwable cause) {
      super(message, cause);
    }

    GetDataException(String message) {
      super(message);
    }
  }
}
