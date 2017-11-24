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

import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.util.IOUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * MultiDataSink allows to save to multiple {@link DataSink}
 *
 * Example usage:
 *<pre>
 * {@code
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
 * }
 *</pre>
 * @param <KEY> key to select DataSink where to save the element
 * @param <IN> key of input element
 */
public class MultiDataSink<KEY, IN> implements DataSink<IN> {

  private final UnaryFunction<IN, KEY> selectFunction;
  private final Map<KEY, DataSinkWrapper<KEY, IN, ?>> sinks;

  private MultiDataSink(
      UnaryFunction<IN, KEY> selectFunction,
      Map<KEY, DataSinkWrapper<KEY, IN, ?>> sinks) {
    this.selectFunction = selectFunction;
    this.sinks = sinks;
  }

  /**
   * Selects DataSink where to save output elements
   * @param selectFunction transform
   * @param <KEY> key to select DataSink
   * @param <IN> key of input element
   * @return Builder
   */
  public static <KEY, IN> Builder<KEY, IN> selectBy(UnaryFunction<IN, KEY> selectFunction) {
    return new Builder<>(selectFunction);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Writer<IN> openWriter(int partitionId) {
    Map<KEY, Writer<Object>> writerMap = new HashMap<>();
    sinks.values().forEach((sink) -> writerMap.put(
        sink.getKey(),
        (Writer<Object>) sink.getDataSink().openWriter(partitionId)));
    return new Writer<IN>() {

      @Override
      public void write(IN elem) throws IOException {
        final KEY key = selectFunction.apply(elem);
        final UnaryFunction<IN, ?> mapper = sinks.get(key).getMapper();
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

  public static class Builder<KEY, IN> {

    private final UnaryFunction<IN, KEY> selectFunction;
    private final List<DataSinkWrapper<KEY, IN, ?>> dataSinkWrappers = new ArrayList<>();

    private Builder(UnaryFunction<IN, KEY> selectFunction) {
      this.selectFunction = selectFunction;
    }

    /**
     *
     * @param key key of elements for sink
     * @param mapper for mapping input to output
     * @param sink added DataSink
     * @param <OUT> key of output element
     * @return Builder
     */
    public <OUT> Builder<KEY, IN> addSink(
        KEY key,
        UnaryFunction<IN, OUT> mapper,
        DataSink<OUT> sink) {
      this.dataSinkWrappers.add(new DataSinkWrapper<>(key, mapper, sink));
      return this;
    }

    public DataSink<IN> build() {
      Map<KEY, DataSinkWrapper<KEY, IN, ?>> sinksMap = new HashMap<>();
      dataSinkWrappers.forEach((el) -> sinksMap.put(el.getKey(), el));
      return new MultiDataSink<>(selectFunction, sinksMap);
    }
  }

  private static class DataSinkWrapper<KEY, IN, OUT> implements Serializable {
    private final KEY key;
    private final UnaryFunction<IN, OUT> mapper;
    private final DataSink<OUT> dataSink;

    DataSinkWrapper(KEY key, UnaryFunction<IN, OUT> mapper, DataSink<OUT> dataSink) {
      this.key = key;
      this.mapper = mapper;
      this.dataSink = dataSink;
    }

    KEY getKey() {
      return key;
    }

    UnaryFunction<IN, OUT> getMapper() {
      return mapper;
    }

    DataSink<OUT> getDataSink() {
      return dataSink;
    }
  }

}