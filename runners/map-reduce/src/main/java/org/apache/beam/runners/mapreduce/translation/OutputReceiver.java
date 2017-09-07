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
package org.apache.beam.runners.mapreduce.translation;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * OutputReceiver that forwards each input it receives to each of a list of down stream operations.
 */
public class OutputReceiver implements Serializable {
  private final List<Operation> receivingOperations = new ArrayList<>();

  /**
   * Adds a new receiver that this OutputReceiver forwards to.
   */
  public void addOutput(Operation receiver) {
    receivingOperations.add(receiver);
  }

  public List<Operation> getReceivingOperations() {
    return ImmutableList.copyOf(receivingOperations);
  }

  /**
   * Processes the element.
   */
  public void process(WindowedValue<?> elem) {
    for (Operation out : receivingOperations) {
      if (out != null) {
        try {
          out.process(elem);
        } catch (IOException | InterruptedException e) {
          Throwables.throwIfUnchecked(e);
          throw new RuntimeException(e);
        }
      }
    }
  }
}
