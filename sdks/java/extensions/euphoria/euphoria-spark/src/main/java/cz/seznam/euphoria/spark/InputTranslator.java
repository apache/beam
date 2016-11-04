package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.core.client.dataset.windowing.Batch;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.executor.FlowUnfolder;
import cz.seznam.euphoria.hadoop.input.DataSourceInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;
import java.util.Arrays;

class InputTranslator implements SparkOperatorTranslator<FlowUnfolder.InputOperator> {

  @Override
  public JavaRDD<?> translate(FlowUnfolder.InputOperator operator,
                              SparkExecutorContext context) {

    // get original datasource from operator
    DataSource<?> ds = operator.output().getSource();

    try {
      Configuration conf = DataSourceInputFormat.configure(
              new Configuration(),
              (DataSource) ds);

      JavaPairRDD<Object, Object> pairs = context.getExecutionEnvironment().newAPIHadoopRDD(
              conf,
              DataSourceInputFormat.class,
              Object.class,
              Object.class);

      // map values to WindowedElement
      return pairs.values().map(v -> new WindowedElement<>(Batch.BatchWindow.get(), v));

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
