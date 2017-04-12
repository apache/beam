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
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.Serializable;

public class DataSinkWrapper<T>
    extends RichSinkFunction<StreamingElement<?, T>>
    implements Checkpointed {

  private DataSink<T> dataSink;
  private Writer<T> writer;

  public DataSinkWrapper(DataSink<T> dataSink) {
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
  public Serializable snapshotState(long l, long l1) throws Exception {
    writer.flush();
    return dataSink;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void restoreState(Serializable t) throws Exception {
    this.dataSink = (DataSink) t;
  }
}
