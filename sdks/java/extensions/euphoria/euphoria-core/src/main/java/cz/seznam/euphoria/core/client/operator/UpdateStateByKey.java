
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.BinaryFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;

import java.util.Optional;

/**
 * Operator that performs the "updateStateByKey" operation on dataset.
 */
public class UpdateStateByKey<IN, KEY, STATE, OUT>
    extends StateAwareElementWiseOperator<IN, KEY, OUT> {


  public static class Builder1<IN> {    
    Dataset<IN> input;
    Builder1(Dataset<IN> input) {
      this.input = input;
    }
    <KEY> Builder2<IN, KEY> by(UnaryFunction<IN, KEY> extractor) {
      return new Builder2<>(input, extractor);
    }
  }  
  public static class Builder2<IN, KEY> {
    final Dataset<IN> input;
    final UnaryFunction<IN, KEY> keyExtractor;
    Builder2(Dataset<IN> input, UnaryFunction<IN, KEY> keyExtractor) {
      this.input = input;
      this.keyExtractor = keyExtractor;
    }
    public <STATE> Builder3<IN, KEY, STATE> using(
        BinaryFunction<Optional<STATE>, IN, STATE> update) {
      return new Builder3<>(input, keyExtractor, update);
    }
  }
  public static class Builder3<IN, KEY, STATE> {
    final Dataset<IN> input;
    final UnaryFunction<IN, KEY> keyExtractor;
    final BinaryFunction<Optional<STATE>, IN, STATE> update;
    Builder3(Dataset<IN> input, UnaryFunction<IN, KEY> keyExtractor,
        BinaryFunction<Optional<STATE>, IN, STATE> update) {
      this.input = input;
      this.keyExtractor = keyExtractor;
      this.update = update;
    }
    public <OUT> UpdateStateByKey<IN, KEY, STATE, OUT> storing(
        UnaryFunction<STATE, OUT> result) {

      Flow flow = input.getFlow();
      UpdateStateByKey<IN, KEY, STATE, OUT> updateStateByKey
          = new UpdateStateByKey<>(flow, input, keyExtractor, update, result);
      return flow.add(updateStateByKey);
    }
  }

  public static <IN> Builder1<IN> of(Dataset<IN> input) {
    return new Builder1<>(input);
  }

  final BinaryFunction<Optional<STATE>, IN, STATE> update;
  final UnaryFunction<STATE, OUT> result;

  UpdateStateByKey(
      Flow flow,
      Dataset<IN> input,
      UnaryFunction<IN, KEY> extractor,
      BinaryFunction<Optional<STATE>, IN, STATE> update,
      UnaryFunction<STATE, OUT> result) {
    
    super("UpdateStateByKey", flow, input, extractor, input.getPartitioning());
    this.update = update;
    this.result = result;
  }

}
