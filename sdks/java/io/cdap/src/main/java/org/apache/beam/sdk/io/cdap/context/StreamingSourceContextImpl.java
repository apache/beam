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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.etl.api.streaming.StreamingSourceContext;
import javax.annotation.Nullable;
import org.apache.tephra.TransactionFailureException;

/** Class for creating context object of different CDAP classes with stream source type. */
public class StreamingSourceContextImpl extends BatchContextImpl implements StreamingSourceContext {

  @Override
  public void registerLineage(String referenceName, @Nullable Schema schema)
      throws DatasetManagementException, TransactionFailureException {}

  @Override
  public boolean isPreviewEnabled() {
    return false;
  }
}
