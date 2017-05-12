package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.operator.ExtractEventTime;

import java.util.Objects;

class EventTimeAssigningUnaryFunctor<WID extends Window, IN, OUT>
    extends UnaryFunctorWrapper<WID, IN, OUT> {

  private final ExtractEventTime<IN> evtTimeFn;

  EventTimeAssigningUnaryFunctor(UnaryFunctor<IN, OUT> functor,
                                 ExtractEventTime<IN> evtTimeFn) {
    super(functor);
    this.evtTimeFn = Objects.requireNonNull(evtTimeFn);
  }

  @Override
  protected long getTimestamp(SparkElement<WID, IN> elem) {
    return evtTimeFn.extractTimestamp(elem.getElement());
  }
}
