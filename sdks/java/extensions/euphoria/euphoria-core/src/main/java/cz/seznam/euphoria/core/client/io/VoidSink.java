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
package cz.seznam.euphoria.core.client.io;

import cz.seznam.euphoria.core.util.Settings;

import java.io.IOException;
import java.net.URI;

public class VoidSink<T> implements DataSink<T> {

  public static class Factory implements DataSinkFactory {
    @Override
    public <T> DataSink<T> get(URI uri, Settings settings) {
      return new VoidSink<T>();
    }
  }

  @Override
  public Writer<T> openWriter(int partitionId) {
    return new Writer<T>() {
      @Override
      public void write(T elem) throws IOException {
        // ~ no-op
      }

      @Override
      public void commit() throws IOException {
        // ~ no-op
      }

      @Override
      public void close() throws IOException {
        // ~ no-op
      }
    };
  }

  @Override
  public void commit() throws IOException {
    // ~ no-op
  }

  @Override
  public void rollback() {
    // ~ no-op
  }

}
