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
package org.apache.beam.runners.tez.translation.io;

import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.library.output.OrderedPartitionedKVOutput;
import org.apache.tez.runtime.library.output.UnorderedKVOutput;

public class OutputManagerFactory {
  public static TezOutputManager createOutputManager(LogicalOutput output){
    TezOutputManager outputManager;
    if (output.getClass().equals(OrderedPartitionedKVOutput.class)){
      outputManager = new OrderedPartitionedKVOutputManager(output);
    } else if (output.getClass().equals(UnorderedKVOutput.class)){
      outputManager = new UnorderedKVEdgeOutputManager(output);
    } else if (output.getClass().equals(MROutput.class)){
      outputManager = new MROutputManager(output);
    } else {
      throw new RuntimeException("Output type: " + output.getClass() + " is unsupported");
    }
    return outputManager;
  }
}
