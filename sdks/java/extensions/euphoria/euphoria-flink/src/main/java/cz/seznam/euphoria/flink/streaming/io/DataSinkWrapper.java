/**
 * Copyright 2016-2017 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.flink.streaming.io;

import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.Writer;
import cz.seznam.euphoria.flink.streaming.StreamingElement;
import cz.seznam.euphoria.shadow.com.google.common.collect.Iterables;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

public class DataSinkWrapper<T>
    extends RichSinkFunction<StreamingElement<?, T>>
    implements CheckpointedFunction {

  private final String storageName;
  private final DataSink<T> dataSink;
  private Writer<T> writer;
  private transient ListState<Writer<T>> state;

  public DataSinkWrapper(String storageName, DataSink<T> dataSink) {
    this.storageName = storageName;
    this.dataSink = dataSink;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    dataSink.initialize();

    RuntimeContext ctx = getRuntimeContext();
    final int subtaskId = ctx.getIndexOfThisSubtask();

    writer = dataSink.openWriter(subtaskId);
  }

  @Override
  public void close() throws Exception {
    if (writer != null) {
      writer.flush();
      writer.commit();
      writer.close();
    }
  }

  @Override
  public void invoke(StreamingElement<?, T> elem) throws Exception {
    writer.write(elem.getElement());
  }

  @Override
  public void snapshotState(FunctionSnapshotContext fsc) throws Exception {
    writer.flush();
    state.clear();
    state.add(writer);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void initializeState(FunctionInitializationContext fic) throws Exception {
    state = (ListState) fic.getOperatorStateStore()
        .getSerializableListState(storageName);
    writer = Iterables.getOnlyElement(state.get(), this.writer);
  }
}
