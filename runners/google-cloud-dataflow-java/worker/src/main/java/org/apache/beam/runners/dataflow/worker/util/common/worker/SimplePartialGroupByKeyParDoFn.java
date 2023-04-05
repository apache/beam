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
package org.apache.beam.runners.dataflow.worker.util.common.worker;

/** A partial group-by-key {@link ParDoFn} implementation. */
public class SimplePartialGroupByKeyParDoFn<K, InputT, AccumT> implements ParDoFn {
  private final GroupingTable<K, InputT, AccumT> groupingTable;
  private final Receiver receiver;

  public SimplePartialGroupByKeyParDoFn(
      GroupingTable<K, InputT, AccumT> groupingTable, Receiver receiver) {
    this.groupingTable = groupingTable;
    this.receiver = receiver;
  }

  @Override
  public void startBundle(Receiver... receivers) throws Exception {}

  @Override
  public void processElement(Object elem) throws Exception {
    groupingTable.put(elem, receiver);
  }

  @Override
  public void processTimers() {}

  @Override
  public void finishBundle() throws Exception {
    groupingTable.flush(receiver);
  }

  @Override
  public void abort() {}
}
