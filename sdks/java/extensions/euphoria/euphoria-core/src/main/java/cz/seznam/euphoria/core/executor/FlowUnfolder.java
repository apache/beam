
package cz.seznam.euphoria.core.executor;

import com.google.common.collect.Iterables;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.graph.Node;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.operator.Operator;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Unfold {@code Flow} to contain only selected operators.
 */
public class FlowUnfolder {

  /**
   * Node added as a producer of inputs. This is fake "operator"
   * with the same input as output.
   */
  public static final class InputOperator<T> extends Operator<T, T> {

    private final Dataset<T> ds;

    InputOperator(Dataset<T> ds) {
      super("InputOperator", ds.getFlow());
      this.ds = ds;
    }

    @Override
    public Collection<Dataset<T>> listInputs() {
      return Collections.EMPTY_LIST;
    }

    @Override
    public Dataset<T> output() {
      return ds;
    }

  }

  /**
   * Unfold the flow so that it contains only allowed operators.
   * Exception is thrown when this is not possible.
   */
  @SuppressWarnings("unchecked")
  public static DAG<Operator<?, ?>> unfold(Flow flow,
      Set<Class<? extends Operator<?, ?>>> operatorClasses)
      throws IllegalArgumentException {

    DAG<Operator<?, ?>> dag = toDAG(flow);
    return translate(dag, (Set) operatorClasses);
  }


  /**
   * Get rid of unwanted operators in a DAG.
   */
  @SuppressWarnings("unchecked")
  private static DAG<Operator<?, ?>> translate(DAG<Operator<?, ?>> dag,
      Set<Class> allowed) throws IllegalArgumentException {

    // create root nodes for all inputs
    DAG<Operator<?, ?>> ret = DAG.of();
    
    Map<Dataset<?>, Optional<Operator<?, ?>>> datasetProducents = new HashMap<>();

    // initialize all other datasets in the original DAG to have empty producents
    dag.nodes().flatMap(n -> n.listInputs().stream())
        .forEach(d -> datasetProducents.put(d, Optional.empty()));
    // next, store the real producents, so that datasets with no producents
    // are stored in 'datasetProducents' without producent
    dag.nodes().forEach(n -> datasetProducents.put(n.output(), Optional.of(n)));

    // filter the dag to contain only specified operators
    dag.bfs().forEach(n -> {
      if (n.get() instanceof InputOperator) {
        // this is added 'fake' operator node, the operator has by definition no
        // parents
        ret.add(n.get());
      } else if (allowed.contains((Class) n.get().getClass())) {
        List<Operator<?, ?>> parents = getParents(n, datasetProducents);
        ret.add(n.get(), parents);
      } else {
        // this is not allowed operator - replace it
        DAG<Operator<?, ?>> basicOps = n.get().getBasicOps();

        // if basicOps contain only single operator, check it is not the
        // original one - that would mean that we cannot convert given flow
        // with given supported operators
        if (basicOps.size() == 1) {
          if (basicOps.nodes().findFirst().get().getClass() == n.get().getClass()) {
            throw new IllegalArgumentException("Operator " + n.get()
                + " cannot be executed with given executor!");
          }
        }

        DAG<Operator<?, ?>> modified = translate(basicOps, allowed);
        
        modified.bfs().forEach(m -> {
          List<Operator<?, ?>> parents = getParents(m, datasetProducents);
          ret.add(m.get(), parents);
          datasetProducents.put(m.get().output(), Optional.of(m.get()));
        });

        Operator<?, ?> leaf = Iterables.getOnlyElement(modified.getLeafs()).get();
        // we have to link the original output dataset with given replaced operator
        datasetProducents.put(n.get().output(), Optional.of(leaf));

        // and propagate output and checkpoint sinks
        if (n.get().output().getOutputSink() != null) {
          leaf.output().persist((DataSink) n.get().output().getOutputSink());
        } else if (n.get().output().getCheckpointSink() != null) {
          leaf.output().checkpoint((DataSink) n.get().output().getCheckpointSink());
        }
      }

    });

    return ret;


  }

  /**
   * Retrieve parent operators in so far constructed transformed DAG.
   */
  private static List<Operator<?, ?>> getParents(
      Node<Operator<?, ?>> node,
      Map<Dataset<?>, Optional<Operator<?, ?>>> datasetProducents) {

    if (node.getParents().isEmpty()) {
      Operator<?, ?> op = node.get();
      List<Operator<?, ?>> parents = op.listInputs()
          .stream()
          .map(i -> datasetProducents.get(i))
          .filter(o -> {
            if (o == null) {
              // there is some strange error
              throw new IllegalStateException("Inputs of operator "
                  + op + " are inconsistent: " + op.listInputs());
            }
            return o.isPresent();
          })
          .map(Optional::get)
          .collect(Collectors.toList());
      return parents;
    }
    return node.getParents().stream()
        .map(n -> datasetProducents.get(n.get().output()))
        .map(o -> {
          if (o == null) {
            throw new IllegalStateException("Output of " + node.get()
                + " should have been stored into 'datasetProducents");
          }
          return o.get();
        })
        .collect(Collectors.toList());
  }


  /**
   * Convert a given {@code Flow} to DAG (unconditionally).
   */
  @SuppressWarnings("unchecked")
  private static DAG<Operator<?, ?>> toDAG(Flow flow) {
    Collection<Operator<?, ?>> operators = flow.operators();
    Set<Operator<?, ?>> resolvedOperators = new HashSet<>();
    Map<Dataset<?>, Operator<?, ?>> datasets = new HashMap<>();
    flow.sources().stream().forEach(d -> datasets.put(d, new InputOperator(d)));

    // root nodes
    List<Operator<?, ?>> roots = datasets.values()
        .stream()
        .collect(Collectors.toList());


    DAG<Operator<?, ?>> ret = DAG.of((List) roots);

    while (resolvedOperators.size() != operators.size()) {
      boolean anyAdded = false;
      for (Operator<?, ?> op : operators) {
        if (!resolvedOperators.contains(op)) {
          if (op.listInputs().stream().allMatch(datasets::containsKey)) {
            // this operator has all inputs available - resolve it
            resolvedOperators.add(op);
            // get parent operators
            List<Operator<?, ?>> parents = op.listInputs().stream()
                .map(datasets::get)
                .collect(Collectors.toList());
            ret.add(op, parents);
            // add output of the operator to available datasets
            datasets.put(op.output(), op);
            anyAdded = true;
          }
        }
      }
      if (!anyAdded) {
        throw new IllegalStateException("Given flow is not a valid DAG!");
      }
    }

    return ret;
  }


}
