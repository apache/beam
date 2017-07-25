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

import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * OutputReceiver that forwards each input it receives to each of a list of down stream
 * ParDoOperations.
 */
public class OutputReceiver implements Serializable {
  private final List<ParDoOperation> receiverParDos = new ArrayList<>();

  /**
   * Adds a new receiver that this OutputReceiver forwards to.
   */
  public void addOutput(ParDoOperation receiver) {
    receiverParDos.add(receiver);
  }

  public List<ParDoOperation> getReceiverParDos() {
    return ImmutableList.copyOf(receiverParDos);
  }

  /**
   * Processes the element.
   */
  public void process(Object elem) {
    for (ParDoOperation out : receiverParDos) {
      if (out != null) {
        out.process(elem);
      }
    }
  }
}
