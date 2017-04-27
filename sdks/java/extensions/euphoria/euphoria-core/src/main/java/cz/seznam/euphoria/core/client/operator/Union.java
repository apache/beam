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
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.annotation.operator.Basic;
import cz.seznam.euphoria.core.annotation.operator.StateComplexity;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

/**
 * The union of two datasets of the same type.<p>
 *
 * In the context of Euphoria, a union is merely a logical view on two datasets
 * as one. One can think of a union as a plain concatenation of two dataset
 * without any guarantees about the order of the datasets' elements. Unlike in
 * set theory, such a union has no notion of uniqueness, i.e. if the two input
 * dataset contain the same elements, these will all appear in the output
 * dataset (as duplicates) untouched.<p>
 *
 * Example:
 *
 * <pre>{@code
 *   Dataset<String> xs = ...;
 *   Dataset<String> ys = ...;
 *   Dataset<String> both = Union.named("XS-AND-YS").of(xs, ys).output();
 * }</pre>
 *
 * The "both" dataset from the above example can now be processed with
 * an operator expecting only a single input dataset, e.g. {@link FlatMap},
 * which will then effectively process both "xs" and "ys".<p>
 *
 * Note: the order of the dataset does not matter. Indeed, the order of the
 * elements themselves in the union is intentionally not specified at all.
 */
@Basic(
    state = StateComplexity.ZERO,
    repartitions = 0
)
public class Union<IN> extends Operator<IN, IN> {

  public static class OfBuilder {
    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    /**
     * Specifies the two datasets to be "unioned".
     *
     * @param <IN> the type of elements in the two datasets
     *
     * @param left one of the two datasets
     * @param right the other of the two datasets
     *
     * @return the next builder to complete the setup of the {@link Union} operator
     */
    public <IN> OutputBuilder<IN> of(Dataset<IN> left, Dataset<IN> right) {
      if (right.getFlow() != left.getFlow()) {
        throw new IllegalArgumentException("Pass inputs from the same flow");
      }
      return new OutputBuilder<>(name, left, right);
    }
  }

  public static class OutputBuilder<IN>
      implements Builders.Output<IN> {
    private final String name;
    private final Dataset<IN> left;
    private final Dataset<IN> right;

    OutputBuilder(String name, Dataset<IN> left, Dataset<IN> right) {
      this.name = Objects.requireNonNull(name);
      this.left = Objects.requireNonNull(left);
      this.right = Objects.requireNonNull(right);
    }

    @Override
    public Dataset<IN> output() {
      Flow flow = left.getFlow();
      Union<IN> union = new Union<>(name, flow, left, right);
      flow.add(union);
      return union.output();
    }
  }

  /**
   * Starts building a nameless Union operator to view two datasets as one.
   *
   * @param <IN> the type of elements in the two datasets
   *
   * @param left one of the two datasets
   * @param right the other of the two datasets
   *
   * @return the next builder to complete the setup of the {@link Union} operator
   *
   * @see #named(String)
   * @see OfBuilder#of(Dataset, Dataset)
   */
  public static <IN> OutputBuilder<IN> of(Dataset<IN> left, Dataset<IN> right) {
    return new OfBuilder("Union").of(left, right);
  }

  /**
   * Starts building a named {@link Union} operator to view two datasets as one.
   *
   * @param name a user provided name of the new operator to build
   *
   * @return the next builder to complete the setup of the new {@link Union} operator
   */
  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  final Dataset<IN> left;
  final Dataset<IN> right;
  final Dataset<IN> output;

  @SuppressWarnings("unchecked")
  Union(String name, Flow flow, Dataset<IN> left, Dataset<IN> right) {
    super(name, flow);
    this.left = Objects.requireNonNull(left);
    this.right = Objects.requireNonNull(right);

    if (left.getFlow() != right.getFlow()) {
      throw new IllegalArgumentException("Pass two datasets from the same flow.");
    }
    this.output = createOutput(left);
  }

  /**
   * Retrieves the single-view dataset representing the union of two input datasets
   *
   * @return the output dataset of this operator
   */
  @Override
  public Dataset<IN> output() {
    return output;
  }

  /**
   * Retrieves the originial, user supplied inputs this union represents.
   *
   * @return a collection of the two input datasets this union represents
   */
  @Override
  public Collection<Dataset<IN>> listInputs() {
    return Arrays.asList(left, right);
  }
}