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
 *   Type getType();
 *   DataFirst getFirst();
 *   DataSecond getSecond();
 *   DataThird getThird();
 * }
 *
 * DataSink<InputElement> = MultiDataSink
 *   .selectBy(InputElement::getType)
 *   .addSink(Type.FIRST, InputElement::getFirst, new SequenceFileSink<>(...))
 *   .addSink(Type.SECOND, InputElement::getSecond, new StdoutSink<>(...))
 *   .addSink(Type.THIRD, InputElement::getThird, new StdoutSink<>(...))
 *   .build();
 * }
 *</pre>
 * @param <SPLIT> key to select DataSink where saves the element
 * @param <IN> type of input element
 */
public class MultiDataSink<SPLIT, IN> implements DataSink<IN> {

  private final UnaryFunction<IN, SPLIT> splitFunction;
  private final Map<SPLIT, DataSinkWrapper<SPLIT, IN, ?>> sinks;

  private MultiDataSink(
      UnaryFunction<IN, SPLIT> splitFunction,
      Map<SPLIT, DataSinkWrapper<SPLIT, IN, ?>> sinks) {
    this.splitFunction = splitFunction;
    this.sinks = sinks;
  }

  /**
   * Selects DataSink where to save output elements
   * @param splitFunction transform
   * @param <SPLIT> key to select DataSink
   * @param <IN> type of input element
   * @return Builder
   */
  public static <SPLIT, IN> Builder<SPLIT, IN> selectBy(UnaryFunction<IN, SPLIT> splitFunction) {
    return new Builder<>(splitFunction);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Writer<IN> openWriter(int partitionId) {
    Map<SPLIT, Writer<Object>> writerMap = new HashMap<>();
    sinks.values().forEach((sink) -> writerMap.put(
        sink.getType(),
        (Writer<Object>) sink.getDataSink().openWriter(partitionId)));
    return new Writer<IN>() {

      @Override
      public void write(IN elem) throws IOException {
        final SPLIT split = splitFunction.apply(elem);
        final UnaryFunction<IN, ?> mapper = sinks.get(split).getMapper();
        writerMap.get(split).write(mapper.apply(elem));
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

  public static class Builder<SPLIT, IN> {

    private final UnaryFunction<IN, SPLIT> splitFunction;
    private final List<DataSinkWrapper<SPLIT, IN, ?>> dataSinkWrappers = new ArrayList<>();

    private Builder(UnaryFunction<IN, SPLIT> splitFunction) {
      this.splitFunction = splitFunction;
    }

    /**
     *
     * @param type type of elements for sink
     * @param mapper for mapping input to ouput
     * @param sink added DataSink
     * @param <OUT> type of output element
     * @return Builder
     */
    public <OUT> Builder<SPLIT, IN> addSink(
        SPLIT type,
        UnaryFunction<IN, OUT> mapper,
        DataSink<OUT> sink) {
      this.dataSinkWrappers.add(new DataSinkWrapper<>(type, mapper, sink));
      return this;
    }

    public DataSink<IN> build() {
      Map<SPLIT, DataSinkWrapper<SPLIT, IN, ?>> sinksMap = new HashMap<>();
      dataSinkWrappers.forEach((el) -> sinksMap.put(el.getType(), el));
      return new MultiDataSink<>(splitFunction, sinksMap);
    }
  }

  private static class DataSinkWrapper<SPLIT, IN, OUT> {
    private final SPLIT type;
    private final UnaryFunction<IN, OUT> mapper;
    private final DataSink<OUT> dataSink;

    public DataSinkWrapper(SPLIT type, UnaryFunction<IN, OUT> mapper, DataSink<OUT> dataSink) {
      this.type = type;
      this.mapper = mapper;
      this.dataSink = dataSink;
    }

    public SPLIT getType() {
      return type;
    }

    public UnaryFunction<IN, OUT> getMapper() {
      return mapper;
    }

    public DataSink<OUT> getDataSink() {
      return dataSink;
    }
  }

}