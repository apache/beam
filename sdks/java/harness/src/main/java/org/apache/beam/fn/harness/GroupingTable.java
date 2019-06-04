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
package org.apache.beam.fn.harness;

/** An interface that groups inputs to an accumulator and flushes the output. */
public interface GroupingTable<K, InputT, AccumT> {

  /** Abstract interface of things that accept inputs one at a time via process(). */
  interface Receiver {
    /** Processes the element. */
    void process(Object outputElem) throws Exception;
  }

  /** Adds a pair to this table, possibly flushing some entries to output if the table is full. */
  void put(Object pair, Receiver receiver) throws Exception;

  /** Flushes all entries in this table to output. */
  void flush(Receiver output) throws Exception;
}
