/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.euphoria.core.client.operator;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.Basic;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.StateComplexity;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Builders;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.hint.OutputHint;
import org.apache.beam.sdk.extensions.euphoria.core.translate.OperatorTransform;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * The union of at least two datasets of the same type.
 *
 * <p>In the context of Euphoria, a union is merely a logical view on two datasets as one. One can
 * think of a union as a plain concatenation of at least two dataset without any guarantees about
 * the order of the datasets' elements. Unlike in set theory, such a union has no notion of
 * uniqueness, i.e. if the two input dataset contain the same elements, these will all appear in the
 * output dataset (as duplicates) untouched.
 *
 * <p>Example:
 *
 * <pre>{@code
 * Dataset<String> xs = ...;
 * Dataset<String> ys = ...;
 * Dataset<String> both = Union.named("XS-AND-YS").of(xs, ys).output();
 * }</pre>
 *
 * <p>The "both" dataset from the above example can now be processed with an operator expecting only
 * a single input dataset, e.g. {@link FlatMap}, which will then effectively process both "xs" and
 * "ys".
 *
 * <p>Note: the order of the dataset does not matter. Indeed, the order of the elements themselves
 * in the union is intentionally not specified at all.
 *
 * <h3>Builders:</h3>
 *
 * <ol>
 *   <li>{@code [named] ..................} give name to the operator [optional]
 *   <li>{@code of .......................} input datasets
 *   <li>{@code output ...................} build output dataset
 * </ol>
 */
@Audience(Audience.Type.CLIENT)
@Basic(state = StateComplexity.ZERO, repartitions = 0)
public class Union<InputT> extends Operator<InputT> {

  /**
   * Starts building a nameless Union operator to view at least two datasets as one.
   *
   * @param <InputT> the type of elements in the data sets
   * @param dataSets at least the two data sets
   * @return the next builder to complete the setup of the {@link Union} operator
   * @see #named(String)
   * @see OfBuilder#of(List)
   */
  @SafeVarargs
  public static <InputT> OutputBuilder<InputT> of(Dataset<InputT>... dataSets) {
    return of(Arrays.asList(dataSets));
  }

  /**
   * Starts building a nameless Union operator to view at least two datasets as one.
   *
   * @param <InputT> the type of elements in the data sets
   * @param dataSets at least the two data sets
   * @return the next builder to complete the setup of the {@link Union} operator
   * @see #named(String)
   * @see OfBuilder#of(List)
   */
  public static <InputT> OutputBuilder<InputT> of(List<Dataset<InputT>> dataSets) {
    return named(null).of(dataSets);
  }

  /**
   * Starts building a named {@link Union} operator to view two datasets as one.
   *
   * @param name a user provided name of the new operator to build
   * @return the next builder to complete the setup of the new {@link Union} operator
   */
  public static OfBuilder named(@Nullable String name) {
    return new Builder<>(name);
  }

  /** Builder for the 'of' step */
  public abstract static class OfBuilder {

    /**
     * Specifies the two data sets to be "unioned".
     *
     * @param <InputT> the type of elements in the two datasets
     * @param dataSets at least two datSets
     * @return the next builder to complete the setup of the {@link Union} operator
     */
    @SafeVarargs
    public final <InputT> OutputBuilder<InputT> of(Dataset<InputT>... dataSets) {
      return of(Arrays.asList(dataSets));
    }

    /**
     * Specifies the two data sets to be "unioned".
     *
     * @param <InputT> the type of elements in the two datasets
     * @param dataSets at least two datSets
     * @return the next builder to complete the setup of the {@link Union} operator
     */
    public abstract <InputT> OutputBuilder<InputT> of(List<Dataset<InputT>> dataSets);
  }

  /**
   * Last builder in a chain. It concludes this operators creation by calling {@link
   * #output(OutputHint...)}.
   */
  public interface OutputBuilder<InputT> extends Builders.Output<InputT> {}

  private static class Builder<InputT> extends OfBuilder implements OutputBuilder<InputT> {

    @Nullable private final String name;
    private List<Dataset<InputT>> dataSets;

    Builder(@Nullable String name) {
      this.name = name;
    }

    @Override
    public <T> OutputBuilder<T> of(List<Dataset<T>> dataSets) {
      @SuppressWarnings("unchecked")
      final Builder<T> casted = (Builder) this;
      casted.dataSets = dataSets;
      return casted;
    }

    @Override
    public Dataset<InputT> output(OutputHint... outputHints) {
      checkArgument(dataSets.size() > 1, "Union needs at least two data sets.");
      return OperatorTransform.apply(
          new Union<>(name, dataSets.get(0).getTypeDescriptor()), dataSets);
    }
  }

  private Union(@Nullable String name, @Nullable TypeDescriptor<InputT> outputType) {
    super(name, outputType);
  }
}
