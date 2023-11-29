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
package org.apache.beam.sdk.extensions.euphoria.core.translate.provider;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.CompositeOperator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.FlatMap;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Join;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.ReduceByKey;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Union;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.translate.CompositeOperatorTranslator;
import org.apache.beam.sdk.extensions.euphoria.core.translate.FlatMapTranslator;
import org.apache.beam.sdk.extensions.euphoria.core.translate.JoinTranslator;
import org.apache.beam.sdk.extensions.euphoria.core.translate.OperatorTranslator;
import org.apache.beam.sdk.extensions.euphoria.core.translate.ReduceByKeyTranslator;
import org.apache.beam.sdk.extensions.euphoria.core.translate.TranslatorProvider;
import org.apache.beam.sdk.extensions.euphoria.core.translate.UnionTranslator;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;

/**
 * Adjustable {@link TranslatorProvider} that selects first suitable translation for the registered
 * operator.
 *
 * <p>{@link OperatorTranslator Translators} can be added by calling variants of {@link
 * GenericTranslatorProvider.Builder#register(Class, OperatorTranslator) register} method during
 * build. Order of registration is important. Building is started by {@link #newBuilder()}.
 *
 * @deprecated Use Java SDK directly, Euphoria is scheduled for removal in a future release.
 */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
@Deprecated
public class GenericTranslatorProvider implements TranslatorProvider {

  public static GenericTranslatorProvider createWithDefaultTranslators() {
    return GenericTranslatorProvider.newBuilder()
        .register(FlatMap.class, new FlatMapTranslator<>())
        .register(Union.class, new UnionTranslator<>())
        .register(ReduceByKey.class, new ReduceByKeyTranslator<>())
        .register(Join.class, new JoinTranslator<>())
        // register fallback operator translator to decompose composite operators
        .register(op -> op instanceof CompositeOperator, new CompositeOperatorTranslator<>())
        .build();
  }

  /**
   * Create a new builder for provider.
   *
   * @return builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /** {@link GenericTranslatorProvider} builder. */
  public static class Builder {

    private final List<TranslationDescriptor> possibleTranslators = new ArrayList<>();

    private Builder() {}

    /**
     * Registers given {@link OperatorTranslator} to be used for given operator type.
     *
     * @param clazz class of an {@link Operator} to be translated
     * @param operatorTranslator translator to register
     * @param <OperatorT> type of an {@link Operator} to be translated
     * @return this builder
     */
    public <OperatorT extends Operator<?>> Builder register(
        Class<OperatorT> clazz, OperatorTranslator<?, ?, ? extends OperatorT> operatorTranslator) {
      possibleTranslators.add(TranslationDescriptor.of(clazz, operatorTranslator));
      return this;
    }

    /**
     * Register given {@link OperatorTranslator} to be used for operator type when given {@link
     * Predicate} holds.
     *
     * @param clazz class of an {@link Operator} to be translated
     * @param predicate user defined predicate which is tested to be true in order to apply
     *     translator to an {@link Operator}
     * @param operatorTranslator translator to register
     * @param <OperatorT> type of an {@link Operator} to be translated
     * @return this builder
     */
    public <OperatorT extends Operator<?>> Builder register(
        Class<OperatorT> clazz,
        Predicate<OperatorT> predicate,
        OperatorTranslator<?, ?, ? extends OperatorT> operatorTranslator) {
      possibleTranslators.add(TranslationDescriptor.of(clazz, predicate, operatorTranslator));
      return this;
    }

    /**
     * Registers given {@link OperatorTranslator} to be used for any operator when given {@link
     * Predicate} holds.
     *
     * @param predicate user defined predicate which is tested to be true in order to apply
     *     translator to an {@link Operator}. Note that predicate have to be able to test any {@link
     *     Operator} instance including subtypes.
     * @param operatorTranslator translator to register
     * @return this builder
     */
    public Builder register(
        Predicate<Operator> predicate, OperatorTranslator<?, ?, Operator> operatorTranslator) {
      possibleTranslators.add(TranslationDescriptor.of(predicate, operatorTranslator));
      return this;
    }

    public GenericTranslatorProvider build() {
      return new GenericTranslatorProvider(possibleTranslators);
    }
  }

  /**
   * Container for optional {@link Predicate user defined predicate}, optional {@link Class} of na
   * operator to be translated and {@link OperatorTranslator} itself. The predicate and operator
   * class defines an optional checks. At least one of them have to be present.
   *
   * <p>The {@link OperatorTranslator} is allowed to translate an operator iff it pass all the
   * checks and {@link OperatorTranslator#canTranslate(Operator) can translate} given operator. That
   * allows users to write translators specific for any operator.
   *
   * @param <OperatorT> the type of the euphoria operator
   */
  private static class TranslationDescriptor<OperatorT extends Operator<?>> {

    /** Class of an {@link Operator} given {@link #translator} can be applied on. */
    private final Optional<Class<OperatorT>> operatorClass;

    /**
     * User specified predicate, which determines whenever given {@link #translator} can be used to
     * translate an operator.
     */
    private final Optional<Predicate<OperatorT>> userDefinedPredicate;

    private final OperatorTranslator<?, ?, OperatorT> translator;

    private TranslationDescriptor(
        Optional<Class<OperatorT>> operatorClass,
        Optional<Predicate<OperatorT>> userDefinedPredicate,
        OperatorTranslator<?, ?, ? extends OperatorT> translator) {
      Preconditions.checkState(
          operatorClass.isPresent() || userDefinedPredicate.isPresent(),
          "At least user defined predicate or class of an operator have to be given.");

      @SuppressWarnings("unchecked")
      OperatorTranslator<?, ?, OperatorT> castTranslator =
          (OperatorTranslator<?, ?, OperatorT>) translator;

      this.operatorClass = operatorClass;
      this.userDefinedPredicate = userDefinedPredicate;
      this.translator = castTranslator;
    }

    static <OperatorT extends Operator<?>> TranslationDescriptor<OperatorT> of(
        Class<OperatorT> operatorClass,
        Predicate<OperatorT> userDefinedPredicate,
        OperatorTranslator<?, ?, ? extends OperatorT> translator) {
      return new TranslationDescriptor<>(
          Optional.of(requireNonNull(operatorClass)),
          Optional.of(requireNonNull(userDefinedPredicate)),
          requireNonNull(translator));
    }

    static <OperatorT extends Operator<?>> TranslationDescriptor<OperatorT> of(
        Predicate<OperatorT> userDefinedPredicate,
        OperatorTranslator<?, ?, ? extends OperatorT> translator) {
      return new TranslationDescriptor<>(
          Optional.empty(), Optional.of(userDefinedPredicate), requireNonNull(translator));
    }

    static <OperatorT extends Operator<?>> TranslationDescriptor<OperatorT> of(
        Class<OperatorT> operatorClass, OperatorTranslator<?, ?, ? extends OperatorT> translator) {
      return new TranslationDescriptor<>(
          Optional.of(requireNonNull(operatorClass)), Optional.empty(), requireNonNull(translator));
    }

    private boolean checkTranslatorSuitableFor(OperatorT operator) {

      // optional class equality check
      if (operatorClass.isPresent() && !operatorClass.get().equals(operator.getClass())) {
        return false;
      }

      // optional user-defined predicate check
      if (userDefinedPredicate.isPresent() && !userDefinedPredicate.get().test(operator)) {
        return false;
      }

      // mandatory check by translator itself
      return translator.canTranslate(operator);
    }

    Optional<OperatorTranslator<?, ?, OperatorT>> getTranslatorWhenSuitable(OperatorT operator) {
      if (checkTranslatorSuitableFor(operator)) {
        return Optional.of(translator);
      } else {
        return Optional.empty();
      }
    }
  }

  private final List<TranslationDescriptor> possibleTranslators;

  private GenericTranslatorProvider(List<TranslationDescriptor> possibleTranslators) {
    this.possibleTranslators = possibleTranslators;
  }

  @Override
  public <InputT, OutputT, OperatorT extends Operator<OutputT>>
      Optional<OperatorTranslator<InputT, OutputT, OperatorT>> findTranslator(OperatorT operator) {

    for (TranslationDescriptor descriptor : possibleTranslators) {

      @SuppressWarnings("unchecked")
      Optional<OperatorTranslator<InputT, OutputT, OperatorT>> maybeTranslator =
          descriptor.getTranslatorWhenSuitable(operator);

      if (maybeTranslator.isPresent()) {
        return maybeTranslator;
      }
    }

    return Optional.empty();
  }
}
