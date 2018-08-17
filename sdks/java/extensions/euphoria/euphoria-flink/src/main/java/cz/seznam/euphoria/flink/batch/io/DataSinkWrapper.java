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
package cz.seznam.euphoria.flink.batch.io;

import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.Writer;
import cz.seznam.euphoria.flink.batch.BatchElement;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;

public class DataSinkWrapper<T>
    implements OutputFormat<BatchElement<?, T>>
{
  private final DataSink<T> dataSink;

  private transient Writer<T> writer;

  public DataSinkWrapper(DataSink<T> dataSink) {
    this.dataSink = dataSink;
  }

  @Override
  public void configure(Configuration parameters) {
    // ignore configuration
  }

  @Override
  public void open(int taskNumber, int numTasks) throws IOException {
    writer = dataSink.openWriter(taskNumber);
  }

  @Override
  public void writeRecord(BatchElement<?, T> record) throws IOException {
    writer.write(record.getElement());
  }

  @Override
  public void close() throws IOException {
    if (writer != null) {
      writer.flush();
      writer.commit();
      writer.close();
    }
  }
}
