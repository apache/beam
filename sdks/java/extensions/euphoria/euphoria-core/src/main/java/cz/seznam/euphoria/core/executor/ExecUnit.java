

package cz.seznam.euphoria.core.executor;

import com.google.common.collect.Sets;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.operator.Operator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An {@code ExecUnit} is a series of transformation with no checkpointing.
 * {@code ExecUnit} has several inputs, several outputs and possibly
 * some intermediate datasets. Datasets might be shared across multiple
 * {@code ExecUnit}s.
 */
public class ExecUnit {

  /** All inputs to this exec unit. */
  final List<Dataset<?>> inputs = new ArrayList<>();
  /** All outputs of this exec unit. */
  final List<Dataset<?>> outputs = new ArrayList<>();
  /** All operators consisting this exec unit. */
  final DAG<Operator<?, ?, ?>> operators;
  /** Flow associated with this unit. */
  final Flow flow;

  /** Split Flow into series of execution units. */
  public static List<ExecUnit> split(Flow flow) {
    Collection<Operator<?, ?, ?>> operators = flow.operators();
    Set<Dataset<?>> availableDatasets = Sets.newHashSet(flow.sources());
    List<ExecUnit> ret = new ArrayList<>();
    List<Operator<?, ?, ?>> currentUnit = new ArrayList<>();
    while (!operators.isEmpty()) {
      Collection<Operator<?, ?, ?>> unfinishedOperators = new ArrayList<>();
      for (Operator<?, ?, ?> op : operators) {
        if (availableDatasets.containsAll(op.listInputs())) {
          availableDatasets.add(op.output());
          currentUnit.add(op);
          boolean isOutput = op.output().getOutputSink() != null
              || op.output().getCheckpointSink() != null;
          if (isOutput) {
            // finish this unit
            ret.add(new ExecUnit(currentUnit, flow));
            currentUnit.clear();
          }
        } else {
          unfinishedOperators.add(op);
        }
      }
      if (operators.size() == unfinishedOperators.size()) {
        // we need to lower the number of unfinished operators
        // otherwise we would end up in an infinite cycle
        throw new IllegalStateException("Invalid flow: " + flow);
      }
      operators = unfinishedOperators;
    }
    return ret;
  }

  private ExecUnit(List<Operator<?, ?, ?>> operators, Flow flow) {
    Set<Dataset<?>> inputDatasets = new HashSet<>();
    Set<Dataset<?>> outputDatasets = new HashSet<>();
    Set<Operator<?, ?, ?>> inputOperators = new HashSet<>();
    for (Operator<?, ?, ?> op : operators) {
      inputDatasets.addAll(
          op.listInputs()
            .stream()
            .filter(d -> d.getSource() != null)
            .collect(Collectors.toList()));
      outputDatasets.addAll(
          op.listInputs()
            .stream()
            .filter(
                d -> d.getCheckpointSink()!= null || d.getOutputSink() != null)
            .collect(Collectors.toList()));
      if (op.listInputs().stream().allMatch(d -> d.getSource() != null)) {
        inputOperators.add(op);
      }
    }
    this.inputs.addAll(inputDatasets);
    this.outputs.addAll(outputDatasets);
    
    this.flow = flow;
    this.operators = DAG.of(
        inputOperators.stream().collect(Collectors.toList()));

    for (Operator<?, ?, ?> op : operators) {
      if (!inputOperators.contains(op)) {
        List<Operator<?, ?, ?>> parents = op.listInputs().stream()
            .filter(i -> i.getProducer() != null)
            .map(Dataset::getProducer)
            .collect(Collectors.toList());
        this.operators.add(op, parents);
      }
    }
  }


  /** Retrieve all inputs of this unit. */
  public Collection<Dataset<?>> getInputs() {
    return inputs;
  }


  /** Retrieve all outputs of this unit. */
  public Collection<Dataset<?>> getOutputs() {
    return outputs;
  }
  

  /** Retrieve exec paths for this unit. */
  public Collection<ExecPath> getPaths() {
    Collection<DAG.Node<Operator<?, ?, ?>>> leafs = operators.getLeafs();
    return leafs.stream()
        .map(l -> ExecPath.of(operators.parentSubGraph(l.get())))
        .collect(Collectors.toList());
  }

}
