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
package org.apache.beam.sdk.transforms;

import java.io.Serializable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

/**
 * {@code Partition} takes a {@code PCollection<T>} and a {@code PartitionFn}, uses the {@code
 * PartitionFn} to split the elements of the input {@code PCollection} into {@code N} partitions,
 * and returns a {@code PCollectionList<T>} that bundles {@code N} {@code PCollection<T>}s
 * containing the split elements.
 *
 * <p>Example of use:
 *
 * <pre>{@code
 * PCollection<Student> students = ...;
 * // Split students up into 10 partitions, by percentile:
 * PCollectionList<Student> studentsByPercentile =
 *     students.apply(Partition.of(10, new PartitionFn<Student>() {
 *         public int partitionFor(Student student, int numPartitions) {
 *             return student.getPercentile()  // 0..99
 *                  * numPartitions / 100;
 *         }}))
 * for (int i = 0; i < 10; i++) {
 *   PCollection<Student> partition = studentsByPercentile.get(i);
 *   ...
 * }
 * }</pre>
 *
 * <pre>{@code
 * PCollection<Student> students = ...;
 * // Split students up into 2 partitions, by percentile based on sideView
 * PCollectionView<Integer> gradesView =
 *         pipeline.apply("grades", Create.of(50)).apply(View.asSingleton());
 * PCollectionList<Integer> studentsByGrades =
 *         pipeline.apply(studentsPercentage)
 *             .apply(Partition.of(2, ((elem, numPartitions, ctx) -> {
 *               Integer grades = ctx.sideInput(gradesView);
 *               return elem < grades ? 0 : 1;
 *             }),Requirements.requiresSideInputs(gradesView)));
 *
 *   PCollection<Student> below = studentsByPercentile.get(0); // all students who are below 50
 *   PCollection<Student> above = studentsByPercentile.get(1); // all students who are 50 or above
 *   ...
 * }
 * }</pre>
 *
 * <p>By default, the {@code Coder} of each of the {@code PCollection}s in the output {@code
 * PCollectionList} is the same as the {@code Coder} of the input {@code PCollection}.
 *
 * <p>Each output element has the same timestamp and is in the same windows as its corresponding
 * input element, and each output {@code PCollection} has the same {@link
 * org.apache.beam.sdk.transforms.windowing.WindowFn} associated with it as the input.
 *
 * @param <T> the type of the elements of the input and output {@code PCollection}s
 */
public class Partition<T> extends PTransform<PCollection<T>, PCollectionList<T>> {

  /**
   * A function object that chooses an output partition for an element.
   *
   * @param <T> the type of the elements being partitioned
   */
  public interface PartitionFn<T> extends Serializable {
    /**
     * Chooses the partition into which to put the given element.
     *
     * @param elem the element to be partitioned
     * @param numPartitions the total number of partitions ({@code >= 1})
     * @return index of the selected partition (in the range {@code [0..numPartitions-1]})
     */
    int partitionFor(T elem, int numPartitions);
  }

  /**
   * A function object that chooses an output partition for an element.
   *
   * @param <T> the type of the elements being partitioned
   */
  public interface PartitionWithSideInputsFn<T> extends Serializable {
    /**
     * Chooses the partition into which to put the given element.
     *
     * @param elem the element to be partitioned
     * @param numPartitions the total number of partitions ({@code >= 1})
     * @param c the {@link Contextful.Fn.Context} needed to access sideInputs.
     * @return index of the selected partition (in the range {@code [0..numPartitions-1]})
     */
    int partitionFor(T elem, int numPartitions, Contextful.Fn.Context c);
  }

  /**
   * Returns a new {@code Partition} {@code PTransform} that divides its input {@code PCollection}
   * into the given number of partitions, using the given partitioning function.
   *
   * @param numPartitions the number of partitions to divide the input {@code PCollection} into
   * @param partitionFn the function to invoke on each element to choose its output partition
   * @param requirements the {@link Requirements} needed to run it.
   * @throws IllegalArgumentException if {@code numPartitions <= 0}
   */
  public static <T> Partition<T> of(
      int numPartitions,
      PartitionWithSideInputsFn<? super T> partitionFn,
      Requirements requirements) {
    Contextful ctfFn =
        Contextful.fn(
            (T element, Contextful.Fn.Context c) ->
                partitionFn.partitionFor(element, numPartitions, c),
            requirements);
    return new Partition<>(new PartitionDoFn<T>(numPartitions, ctfFn, partitionFn));
  }

  /**
   * Returns a new {@code Partition} {@code PTransform} that divides its input {@code PCollection}
   * into the given number of partitions, using the given partitioning function.
   *
   * @param numPartitions the number of partitions to divide the input {@code PCollection} into
   * @param partitionFn the function to invoke on each element to choose its output partition
   * @throws IllegalArgumentException if {@code numPartitions <= 0}
   */
  public static <T> Partition<T> of(int numPartitions, PartitionFn<? super T> partitionFn) {

    Contextful ctfFn =
        Contextful.fn(
            (T element, Contextful.Fn.Context c) ->
                partitionFn.partitionFor(element, numPartitions),
            Requirements.empty());

    return new Partition<>(new PartitionDoFn<T>(numPartitions, ctfFn, partitionFn));
  }

  /////////////////////////////////////////////////////////////////////////////

  @Override
  public PCollectionList<T> expand(PCollection<T> in) {
    final TupleTagList outputTags = partitionDoFn.getOutputTags();

    PCollectionTuple outputs =
        in.apply(
            ParDo.of(partitionDoFn)
                .withOutputTags(new TupleTag<Void>() {}, outputTags)
                .withSideInputs(partitionDoFn.getSideInputs()));

    PCollectionList<T> pcs = PCollectionList.empty(in.getPipeline());
    Coder<T> coder = in.getCoder();

    for (TupleTag<?> outputTag : outputTags.getAll()) {
      // All the tuple tags are actually TupleTag<T>
      // And all the collections are actually PCollection<T>
      @SuppressWarnings("unchecked")
      TupleTag<T> typedOutputTag = (TupleTag<T>) outputTag;
      pcs = pcs.and(outputs.get(typedOutputTag).setCoder(coder));
    }
    return pcs;
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.include("partitionFn", partitionDoFn);
  }

  private final transient PartitionDoFn<T> partitionDoFn;

  private Partition(PartitionDoFn<T> partitionDoFn) {
    this.partitionDoFn = partitionDoFn;
  }

  private static class PartitionDoFn<X> extends DoFn<X, Void> {
    private final int numPartitions;
    private final TupleTagList outputTags;
    private final Contextful<Contextful.Fn<X, Integer>> ctxFn;
    private final Object originalFnClassForDisplayData;

    /**
     * Constructs a PartitionDoFn.
     *
     * @throws IllegalArgumentException if {@code numPartitions <= 0}
     */
    private PartitionDoFn(
        int numPartitions,
        Contextful<Contextful.Fn<X, Integer>> ctxFn,
        Object originalFnClassForDisplayData) {
      this.ctxFn = ctxFn;
      this.originalFnClassForDisplayData = originalFnClassForDisplayData;
      if (numPartitions <= 0) {
        throw new IllegalArgumentException("numPartitions must be > 0");
      }

      this.numPartitions = numPartitions;

      TupleTagList buildOutputTags = TupleTagList.empty();
      for (int partition = 0; partition < numPartitions; partition++) {
        buildOutputTags = buildOutputTags.and(new TupleTag<X>());
      }
      outputTags = buildOutputTags;
    }

    public TupleTagList getOutputTags() {
      return outputTags;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      X input = c.element();
      int partition = ctxFn.getClosure().apply(input, Contextful.Fn.Context.wrapProcessContext(c));
      if (0 <= partition && partition < numPartitions) {
        @SuppressWarnings("unchecked")
        TupleTag<X> typedTag = (TupleTag<X>) outputTags.get(partition);
        c.output(typedTag, input);
      } else {
        throw new IndexOutOfBoundsException(
            "Partition function returned out of bounds index: "
                + partition
                + " not in [0.."
                + numPartitions
                + ")");
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .add(DisplayData.item("numPartitions", numPartitions).withLabel("Partition Count"))
          .add(
              DisplayData.item("partitionFn", originalFnClassForDisplayData.getClass())
                  .withLabel("Partition Function"));
    }

    public Iterable<? extends PCollectionView<?>> getSideInputs() {
      return ctxFn.getRequirements().getSideInputs();
    }
  }
}
