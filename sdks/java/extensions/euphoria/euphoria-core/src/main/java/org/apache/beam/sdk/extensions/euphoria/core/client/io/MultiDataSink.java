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
package org.apache.beam.sdk.extensions.euphoria.core.client.io;

import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.util.IOUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * MultiDataSink allows to save to multiple {@link DataSink}.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * interface InputElement {
 *   enum Type { FIRST, SECOND, THIRD }
 *   Type getKey();
 *   DataFirst getFirst();
 *   DataSecond getSecond();
 *   DataThird getThird();
 * }
 *
 * DataSink<InputElement> = MultiDataSink
 *   .selectBy(InputElement::getKey)
 *   .addSink(Type.FIRST, InputElement::getFirst, new SequenceFileSink<>(...))
 *   .addSink(Type.SECOND, InputElement::getSecond, new StdoutSink<>(...))
 *   .addSink(Type.THIRD, InputElement::getThird, new StdoutSink<>(...))
 *   .build();
 * }</pre>
 *
 * @param <K> key to select DataSink where to save the element
 * @param <InputT> key of input element
 */
public class MultiDataSink<K, InputT> implements DataSink<InputT> {

  private final UnaryFunction<InputT, K> selectFunction;
  private final Map<K, DataSinkWrapper<K, InputT, ?>> sinks;

  private MultiDataSink(
      UnaryFunction<InputT, K> selectFunction, Map<K, DataSinkWrapper<K, InputT, ?>> sinks) {
    this.selectFunction = selectFunction;
    this.sinks = sinks;
  }

  /**
   * Selects DataSink where to save output elements.
   *
   * @param selectFunction transform
   * @param <K> key to select DataSink
   * @param <InputT> key of input element
   * @return Builder
   */
  public static <K, InputT> Builder<K, InputT> selectBy(UnaryFunction<InputT, K> selectFunction) {
    return new Builder<>(selectFunction);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Writer<InputT> openWriter(int partitionId) {
    Map<K, Writer<Object>> writerMap = new HashMap<>();
    sinks
        .values()
        .forEach(
            (sink) ->
                writerMap.put(
                    sink.getKey(), (Writer<Object>) sink.getDataSink().openWriter(partitionId)));
    return new Writer<InputT>() {

      @Override
      public void write(InputT elem) throws IOException {
        final K key = selectFunction.apply(elem);
        final UnaryFunction<InputT, ?> mapper = sinks.get(key).getMapper();
        writerMap.get(key).write(mapper.apply(elem));
      }

      @Override
      public void commit() throws IOException {
        IOUtils.forEach(writerMap.values(), Writer::commit);
      }

      @Override
      public void close() throws IOException {
        IOUtils.forEach(writerMap.values(), Writer::close);
      }
    };
  }

  @Override
  public void commit() throws IOException {
    IOUtils.forEach(sinks.values().stream().map(DataSinkWrapper::getDataSink), DataSink::commit);
  }

  @Override
  public void rollback() throws IOException {
    IOUtils.forEach(sinks.values().stream().map(DataSinkWrapper::getDataSink), DataSink::rollback);
  }

  /**
   * @param <K>
   * @param <InputT>
   */
  public static class Builder<K, InputT> {

    private final UnaryFunction<InputT, K> selectFunction;
    private final List<DataSinkWrapper<K, InputT, ?>> dataSinkWrappers = new ArrayList<>();

    private Builder(UnaryFunction<InputT, K> selectFunction) {
      this.selectFunction = selectFunction;
    }

    /**
     * @param key key of elements for sink
     * @param mapper for mapping input to output
     * @param sink added DataSink
     * @param <OutputT> key of output element
     * @return Builder
     */
    public <OutputT> Builder<K, InputT> addSink(
        K key, UnaryFunction<InputT, OutputT> mapper, DataSink<OutputT> sink) {
      this.dataSinkWrappers.add(new DataSinkWrapper<>(key, mapper, sink));
      return this;
    }

    public DataSink<InputT> build() {
      Map<K, DataSinkWrapper<K, InputT, ?>> sinksMap = new HashMap<>();
      dataSinkWrappers.forEach((el) -> sinksMap.put(el.getKey(), el));
      return new MultiDataSink<>(selectFunction, sinksMap);
    }
  }

  private static class DataSinkWrapper<K, InputT, OutputT> implements Serializable {
    private final K key;
    private final UnaryFunction<InputT, OutputT> mapper;
    private final DataSink<OutputT> dataSink;

    DataSinkWrapper(K key, UnaryFunction<InputT, OutputT> mapper, DataSink<OutputT> dataSink) {
      this.key = key;
      this.mapper = mapper;
      this.dataSink = dataSink;
    }

    K getKey() {
      return key;
    }

    UnaryFunction<InputT, OutputT> getMapper() {
      return mapper;
    }

    DataSink<OutputT> getDataSink() {
      return dataSink;
    }
  }
}
