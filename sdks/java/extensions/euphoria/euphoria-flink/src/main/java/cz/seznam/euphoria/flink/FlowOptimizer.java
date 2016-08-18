package cz.seznam.euphoria.flink;

import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.client.operator.PartitioningAware;
import cz.seznam.euphoria.core.executor.FlowUnfolder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Converts DAG of Euphoria {@link Operator} to Flink-layer specific
 * DAG of {@link FlinkOperator}. During the conversion some
 * kind of optimization can be made.
 */
public class FlowOptimizer {

  private int maxParallelism = Integer.MAX_VALUE;

  public int getMaxParallelism() {
    return maxParallelism;
  }

  public void setMaxParallelism(int maxParallelism) {
    this.maxParallelism = maxParallelism;
  }

  public DAG<FlinkOperator<?>> optimize(DAG<Operator<?, ?>> dag) {
    DAG<FlinkOperator<?>> flinkDag = convert(dag);

    // setup parallelism
    return setParallelism(flinkDag);
  }

  /**
   * Converts DAG of Euphoria {@link Operator} to Flink-layer specific
   * DAG of {@link FlinkOperator}.
   */
  private DAG<FlinkOperator<?>> convert(DAG<Operator<?, ?>> dag) {
    DAG<FlinkOperator<?>> output = DAG.of();

    // mapping between original operator and newly created executor
    // specific wrapper
    final Map<Operator<?, ?>, FlinkOperator<?>> mapping = new HashMap<>();

    dag.traverse().forEach(n -> {
      Operator<?, ?> current = n.get();
      FlinkOperator<?> created = new FlinkOperator<>(current);
      mapping.put(current, created);

      List<FlinkOperator<?>> parents = n.getParents().stream().map(
          p -> mapping.get(p.get())).collect(Collectors.toList());
      output.add(created, parents);
    });

    return output;
  }

  /**
   * Modifies given DAG in a way that all operators
   * will have the parallelism explicitly defined.
   * @param dag Original DAG
   * @return Modified DAG
   */
  private DAG<FlinkOperator<?>> setParallelism(DAG<FlinkOperator<?>> dag) {
    dag.traverse().forEach(n -> {
      FlinkOperator flinkOp = n.get();
      Operator<?, ?> op = flinkOp.getOriginalOperator();

      if (op instanceof FlowUnfolder.InputOperator) {
        int partitions = op.output().getSource().getPartitions().size();
        flinkOp.setParallelism(Math.min(maxParallelism, partitions));
      } else if (op instanceof PartitioningAware) {
        int partitions = ((PartitioningAware) op).getPartitioning().getNumPartitions();
        flinkOp.setParallelism(Math.min(maxParallelism, partitions));
      } else {
        // other operators inherit parallelism from their parents
        flinkOp.setParallelism(
                n.getParents().stream().mapToInt(
                        p -> p.get().getParallelism()).max().getAsInt());
      }
    });

    return dag;
  }
}
