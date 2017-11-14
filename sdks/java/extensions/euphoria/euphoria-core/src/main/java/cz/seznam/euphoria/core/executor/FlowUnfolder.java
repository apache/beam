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
package cz.seznam.euphoria.core.executor;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryPredicate;
import cz.seznam.euphoria.core.executor.graph.DAG;
import cz.seznam.euphoria.core.executor.graph.Node;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.operator.Operator;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
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
@Audience(Audience.Type.EXECUTOR)
public class FlowUnfolder {

  /**
   * Node added as a producer of inputs. This is dummy "operator"
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
      return Collections.emptyList();
    }

    @Override
    public Dataset<T> output() {
      return ds;
    }

  }

  /**
   * Unfolds the flow so that it contains only allowed operators.
   *
   * @param flow original flow
   * @param operatorClasses allowed operators
   *
   * @return a dag representing the given flow in an "unfolded" form
   *
   * @throws IllegalArgumentException when the transformation is not possible.
   */
  @SuppressWarnings("unchecked")
  public static DAG<Operator<?, ?>> unfold(Flow flow,
      Set<Class<? extends Operator<?, ?>>> operatorClasses)
      throws IllegalArgumentException {
    return unfold(flow, op -> operatorClasses.contains(op.getClass()));
  }

  /**
   * Unfolds the flow so that it contains only operators for which
   * {@code wantTranslate} returns {@code true}. Operators for which
   * {@code wantTranslate} doesn't return {@code true} are expanded
   * into their {@link Operator#getBasicOps()} and {@code wantTranslate}
   * will be recursively called on these. Hence, there is a certain set
   * of basic operators which the {@code wantTranslate} function has to accept.
   *
   * @param flow the original flow to be unfolded
   * @param wantTranslate user defined function determining which operators
   *         the caller wants to translate itself without being further expanded
   *         into their basic operations
   *
   * @return the unfolded/expanded version of the given flow
   */
  public static DAG<Operator<?, ?>> unfold(
      Flow flow, UnaryPredicate<Operator<?, ?>> wantTranslate) {
    DAG<Operator<?, ?>> dag = toDAG(flow);
    return translate(dag, wantTranslate);
  }

  /**
   * Translates the given DAG to a DAG of basic operators.
   *
   * @param dag the original DAG
   * @param wantTranslate predicate determining whether a particular
   *         operator instance will be translated separately and, thus,
   *         should be left in the resulting DAG or whether it is to
   *         be expanded into its basic ops.
   *
   * @return the translated DAG consisting of basic operators only
   */
  @SuppressWarnings("unchecked")
  private static DAG<Operator<?, ?>> translate(
      DAG<Operator<?, ?>> dag,
      UnaryPredicate<Operator<?, ?>> wantTranslate)
      throws IllegalArgumentException {

    dag = FlowValidator.preTranslate(dag);

    // create root nodes for all inputs
    DAG<Operator<?, ?>> ret = DAG.of();

    Map<Dataset<?>, Optional<Operator<?, ?>>> datasetProducers = new HashMap<>();

    // initialize all other datasets in the original DAG to have empty producers
    dag.nodes()
        .flatMap(n -> n.listInputs().stream())
        .forEach(d -> datasetProducers.put(d, Optional.empty()));

    // next, store the real producers, so that datasets with no producers
    // are stored in 'datasetProducers' without producer
    dag.nodes().forEach(n -> datasetProducers.put(n.output(), Optional.of(n)));

    // filter the dag to contain only specified operators
    dag.traverse().forEach(n -> {
      if (n.get() instanceof InputOperator) {
        // this is added 'dummy' operator node, the operator has by definition no
        // parents
        ret.add(n.get());
      } else if (wantTranslate.apply(n.get())) {
        List<Operator<?, ?>> parents = getParents(n, datasetProducers);
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

        DAG<Operator<?, ?>> modified = translate(basicOps, wantTranslate);

        modified.traverse().forEach(m -> {
          List<Operator<?, ?>> parents = getParents(m, datasetProducers);
          ret.add(m.get(), parents);
          datasetProducers.put(m.get().output(), Optional.of(m.get()));
        });

        Operator<?, ?> leaf = Iterables.getOnlyElement(modified.getLeafs()).get();
        // we have to link the original output dataset with given replaced operator
        datasetProducers.put(n.get().output(), Optional.of(leaf));

        // and propagate output sinks
        if (n.get().output().getOutputSink() != null) {
          leaf.output().persist((DataSink) n.get().output().getOutputSink());
        }
      }

    });

    return FlowValidator.postTranslate(ret);
  }

  /*
   * Retrieve parent operators in so far constructed transformed DAG.
   */
  private static List<Operator<?, ?>> getParents(
      Node<Operator<?, ?>> node,
      Map<Dataset<?>, Optional<Operator<?, ?>>> datasetProducents) {

    if (node.getParents().isEmpty()) {
      Operator<?, ?> op = node.get();
      return op.listInputs()
          .stream()
          .map(datasetProducents::get)
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


  /*
   * Convert a given {@code Flow} to DAG (unconditionally).
   */
  @SuppressWarnings("unchecked")
  private static DAG<Operator<?, ?>> toDAG(Flow flow) {

    // let output sinks modify the Flow
    applySinkTransforms(flow);

    Collection<Operator<?, ?>> operators = flow.operators();
    Set<Operator<?, ?>> resolvedOperators = new HashSet<>();
    Map<Dataset<?>, Operator<?, ?>> datasets = new HashMap<>();
    flow.sources().forEach(d -> datasets.put(d, new InputOperator(d)));

    // root nodes
    List<Operator<?, ?>> roots = new ArrayList<>(datasets.values());

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


  @SuppressWarnings("unchecked")
  private static void applySinkTransforms(Flow flow) {
    List<Dataset<?>> outputs = flow.operators().stream()
        .filter(o -> o.output().getOutputSink() != null)
        .map(Operator::output)
        .collect(Collectors.toList());

    outputs.forEach(d -> {
      if (d.getOutputSink().prepareDataset((Dataset) d)) {
        // remove the old output sink
        d.persist(null);
      }
    });
  }

}
