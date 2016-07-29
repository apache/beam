package cz.seznam.euphoria.flink.translation;

import cz.seznam.euphoria.core.client.operator.Operator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.datastream.DataStream;

interface OperatorTranslator<T extends Operator> {

  /**
   * Translates Euphoria {@code Operator} to Flink transformation
   * @param operator Euphoria operator
   * @param context Processing context aware of all inputs of given operator
   * @return Output of transformation in Flink API
   */
  DataStream translate(T operator, TranslationContext context);
}
