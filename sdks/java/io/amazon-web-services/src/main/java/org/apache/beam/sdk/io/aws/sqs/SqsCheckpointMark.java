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
package org.apache.beam.sdk.io.aws.sqs;

import com.amazonaws.services.sqs.model.Message;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;

class SqsCheckpointMark implements UnboundedSource.CheckpointMark, Serializable {

  private final List<Message> messagesToDelete;
  private final transient Optional<SqsUnboundedReader> reader;

  public SqsCheckpointMark(SqsUnboundedReader reader, Collection<Message> messagesToDelete) {
    this.reader = Optional.of(reader);
    this.messagesToDelete = ImmutableList.copyOf(messagesToDelete);
    ;
  }

  @Override
  public void finalizeCheckpoint() {
    reader.ifPresent(r -> r.delete(messagesToDelete));
  }

  List<Message> getMessagesToDelete() {
    return messagesToDelete;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SqsCheckpointMark that = (SqsCheckpointMark) o;
    return Objects.equal(messagesToDelete, that.messagesToDelete);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(messagesToDelete);
  }
}
