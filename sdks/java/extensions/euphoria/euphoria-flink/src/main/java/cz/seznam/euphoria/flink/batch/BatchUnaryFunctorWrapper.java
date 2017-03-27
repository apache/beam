/**
 * Copyright 2016 Seznam.cz, a.s.
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
package cz.seznam.euphoria.flink.batch;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.io.Context;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

import java.util.Objects;

public class BatchUnaryFunctorWrapper<WID extends Window, IN, OUT>
        implements FlatMapFunction<BatchElement<WID, IN>,
        BatchElement<WID, OUT>>,
        ResultTypeQueryable<BatchElement<WID, OUT>> {

  private final UnaryFunctor<IN, OUT> f;

  public BatchUnaryFunctorWrapper(UnaryFunctor<IN, OUT> f) {
    this.f = Objects.requireNonNull(f);
  }

  @Override
  public void flatMap(BatchElement<WID, IN> value,
                      Collector<BatchElement<WID, OUT>> out)
      throws Exception
  {
    f.apply(value.getElement(), new Context<OUT>() {
      @Override
      public void collect(OUT elem) {
        out.collect(new BatchElement<>(
                value.getWindow(), value.getTimestamp(), elem));
      }
      @Override
      public Object getWindow() {
        return value.getWindow();
      }
    });
  }

  @SuppressWarnings("unchecked")
  @Override
  public TypeInformation<BatchElement<WID, OUT>> getProducedType() {
    return TypeInformation.of((Class) BatchElement.class);
  }
}
