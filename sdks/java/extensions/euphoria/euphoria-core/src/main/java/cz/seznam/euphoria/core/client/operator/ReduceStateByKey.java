
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.GroupedDataset;
import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowContext;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.StateFactory;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.util.Pair;

import java.util.Objects;

/**
 * Operator reducing state by given reduce function.
 *
 * @param <IN>     Type of input records
 * @param <KIN>    Type of records entering #keyBy and #valueBy methods
 * @param <WIN>    Type of input records being windowed
 * @param <KEY>    Output type of #keyBy method
 * @param <VALUE>  Output type of #valueBy method
 * @param <KEYOUT> Type of output key
 * @param <OUT>    Type of output value
 */
public class ReduceStateByKey<
    IN, KIN, WIN, KEY, VALUE, KEYOUT, OUT, STATE extends State<VALUE, OUT>,
    WLABEL, W extends WindowContext<WLABEL>>
    extends StateAwareWindowWiseSingleInputOperator<IN, WIN, KIN, KEY,
        Pair<KEYOUT, OUT>, WLABEL, W,
        ReduceStateByKey<IN, KIN, WIN, KEY, VALUE, KEYOUT, OUT, STATE, WLABEL, W>>
{

  public static class OfBuilder {
    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    public <IN> DatasetBuilder1<IN> of(Dataset<IN> input) {
      return new DatasetBuilder1<>(name, input);
    }

    public <IN, KIN> GroupedDatasetBuilder1<IN, KIN> of(GroupedDataset<IN, KIN> input) {
      return new GroupedDatasetBuilder1<>(name, input);
    }
  }

  // builder classes used when input is Dataset<IN> ----------------------

  public static class DatasetBuilder1<IN> {
    private final String name;
    private final Dataset<IN> input;
    DatasetBuilder1(String name, Dataset<IN> input) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
    }
    public <KEY> DatasetBuilder2<IN, KEY> keyBy(UnaryFunction<IN, KEY> keyExtractor) {
      return new DatasetBuilder2<>(name, input, keyExtractor);
    }
  }
  public static class DatasetBuilder2<IN, KEY> {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, KEY> keyExtractor;
    DatasetBuilder2(String name, Dataset<IN> input, UnaryFunction<IN, KEY> keyExtractor) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
    }
    public <VALUE> DatasetBuilder3<IN, KEY, VALUE> valueBy(UnaryFunction<IN, VALUE> valueExtractor) {
      return new DatasetBuilder3<>(name, input, keyExtractor, valueExtractor);
    }
  }
  public static class DatasetBuilder3<IN, KEY, VALUE> {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, KEY> keyExtractor;
    private final UnaryFunction<IN, VALUE> valueExtractor;
    DatasetBuilder3(String name,
                    Dataset<IN> input,
                    UnaryFunction<IN, KEY> keyExtractor,
                    UnaryFunction<IN, VALUE> valueExtractor)
    {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
      this.valueExtractor = Objects.requireNonNull(valueExtractor);
    }
    public <OUT, STATE extends State<VALUE, OUT>> DatasetBuilder4<
            IN, KEY, VALUE, OUT, STATE> stateFactory(
            StateFactory<OUT, STATE> stateFactory) {
      return new DatasetBuilder4<>(
          name, input, keyExtractor, valueExtractor, stateFactory);
    }
  }
  public static class DatasetBuilder4<
      IN, KEY, VALUE, OUT, STATE extends State<VALUE, OUT>> {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, KEY> keyExtractor;
    private final UnaryFunction<IN, VALUE> valueExtractor;
    private final StateFactory<OUT, STATE> stateFactory;
    DatasetBuilder4(String name,
                    Dataset<IN> input,
                    UnaryFunction<IN, KEY> keyExtractor,
                    UnaryFunction<IN, VALUE> valueExtractor,
                    StateFactory<OUT, STATE> stateFactory)
    {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
      this.valueExtractor = Objects.requireNonNull(valueExtractor);
      this.stateFactory = Objects.requireNonNull(stateFactory);
    }
    public DatasetBuilder5<IN, KEY, VALUE, OUT, STATE> combineStateBy(
        CombinableReduceFunction<STATE> stateCombiner) {
      return new DatasetBuilder5<>(name, input, keyExtractor, valueExtractor,
              stateFactory, stateCombiner);
    }
  }
  public static class DatasetBuilder5<
      IN, KEY, VALUE, OUT, STATE extends State<VALUE, OUT>>
          extends PartitioningBuilder<KEY,  DatasetBuilder5<IN, KEY, VALUE, OUT, STATE>>
      implements OutputBuilder<Pair<KEY, OUT>>
  {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, KEY> keyExtractor;
    private final UnaryFunction<IN, VALUE> valueExtractor;
    private final StateFactory<OUT, STATE> stateFactory;
    private final CombinableReduceFunction<STATE> stateCombiner;

    DatasetBuilder5(String name,
                    Dataset<IN> input,
                    UnaryFunction<IN, KEY> keyExtractor,
                    UnaryFunction<IN, VALUE> valueExtractor,
                    StateFactory<OUT, STATE> stateFactory,
                    CombinableReduceFunction<STATE> stateCombiner)
    {
      // initialize default partitioning according to input
      super(new DefaultPartitioning<>(input.getPartitioning().getNumPartitions()));

      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
      this.valueExtractor = Objects.requireNonNull(valueExtractor);
      this.stateFactory = Objects.requireNonNull(stateFactory);
      this.stateCombiner = Objects.requireNonNull(stateCombiner);
    }
    public <WIN, WLABEL, W extends WindowContext<WLABEL>>
    DatasetBuilder6<IN, WIN, KEY, VALUE, OUT, STATE, WLABEL, W>
    windowBy(Windowing<WIN, WLABEL, W> windowing)
    {
      return new DatasetBuilder6<>(name, input, keyExtractor, valueExtractor,
              stateFactory, stateCombiner, Objects.requireNonNull(windowing), this);
    }
    @Override
    public Dataset<Pair<KEY, OUT>> output() {
      return new DatasetBuilder6<>(name, input, keyExtractor, valueExtractor,
          stateFactory, stateCombiner, null, this)
          .output();
    }
  }

  public static class DatasetBuilder6<
          IN, WIN, KEY, VALUE, OUT, STATE extends State<VALUE, OUT>,
          WLABEL, W extends WindowContext<WLABEL>>
      extends PartitioningBuilder<
          KEY,DatasetBuilder6<IN, WIN, KEY, VALUE, OUT, STATE, WLABEL, W>>
      implements OutputBuilder<Pair<KEY, OUT>>
  {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, KEY> keyExtractor;
    private final UnaryFunction<IN, VALUE> valueExtractor;
    private final StateFactory<OUT, STATE> stateFactory;
    private final CombinableReduceFunction<STATE> stateCombiner;
    private final Windowing<WIN, WLABEL, W> windowing;

    DatasetBuilder6(String name,
                    Dataset<IN> input,
                    UnaryFunction<IN, KEY> keyExtractor,
                    UnaryFunction<IN, VALUE> valueExtractor,
                    StateFactory<OUT, STATE> stateFactory,
                    CombinableReduceFunction<STATE> stateCombiner,
                    Windowing<WIN, WLABEL, W> windowing /* optional */,
                    PartitioningBuilder<KEY, ?> partitioning)
    {
      // initialize partitioning
      super(partitioning);

      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
      this.valueExtractor = Objects.requireNonNull(valueExtractor);
      this.stateFactory = Objects.requireNonNull(stateFactory);
      this.stateCombiner = Objects.requireNonNull(stateCombiner);
      this.windowing = windowing;
    }

    @Override
    public Dataset<Pair<KEY, OUT>> output() {
      Flow flow = input.getFlow();

      ReduceStateByKey<IN, IN, WIN, KEY, VALUE, KEY, OUT, STATE, WLABEL, W>
          reduceStateByKey =
          new ReduceStateByKey<>(name, flow, input, keyExtractor, valueExtractor,
              windowing, stateFactory, stateCombiner, getPartitioning());
      flow.add(reduceStateByKey);

      return reduceStateByKey.output();
    }
  }

  // builder classes used for GroupedDataset<K, V> input -------------------

  public static class GroupedDatasetBuilder1<IN, KIN> {
    private final String name;
    private final GroupedDataset<IN, KIN> input;
    GroupedDatasetBuilder1(String name, GroupedDataset<IN, KIN> input) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
    }
    public <KEY> GroupedDatasetBuilder2<IN, KIN, KEY> keyBy(UnaryFunction<KIN, KEY> keyExtractor) {
      return new GroupedDatasetBuilder2<>(name, input, keyExtractor);
    }
  }
  public static class GroupedDatasetBuilder2<IN, KIN, KEY> {
    private final String name;
    private final GroupedDataset<IN, KIN> input;
    private final UnaryFunction<KIN, KEY> keyExtractor;
    GroupedDatasetBuilder2(String name, GroupedDataset<IN, KIN> input, UnaryFunction<KIN, KEY> keyExtractor) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
    }
    public <VALUE> GroupedDatasetBuilder3<IN, KIN, KEY, VALUE> valueBy(UnaryFunction<KIN, VALUE> valueExtractor) {
      return new GroupedDatasetBuilder3<>(name, input, keyExtractor, valueExtractor);
    }
  }
  public static class GroupedDatasetBuilder3<IN, KIN, KEY, VALUE> {
    private final String name;
    private final GroupedDataset<IN, KIN> input;
    private final UnaryFunction<KIN, KEY> keyExtractor;
    private final UnaryFunction<KIN, VALUE> valueExtractor;
    GroupedDatasetBuilder3(String name,
                           GroupedDataset<IN, KIN> input,
                           UnaryFunction<KIN, KEY> keyExtractor,
                           UnaryFunction<KIN, VALUE> valueExtractor)
    {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
      this.valueExtractor = Objects.requireNonNull(valueExtractor);
    }
    public <OUT, STATE extends State<VALUE, OUT>> GroupedDatasetBuilder4<
            IN, KIN, KEY, VALUE, OUT, STATE> stateFactory(
            StateFactory<OUT, STATE> stateFactory) {
      return new GroupedDatasetBuilder4<>(
          name, input, keyExtractor, valueExtractor, stateFactory);
    }
  }
  public static class GroupedDatasetBuilder4<
          IN, KIN, KEY, VALUE, OUT, STATE extends State<VALUE, OUT>> {
    private final String name;
    private final GroupedDataset<IN, KIN> input;
    private final UnaryFunction<KIN, KEY> keyExtractor;
    private final UnaryFunction<KIN, VALUE> valueExtractor;
    private final StateFactory<OUT, STATE> stateFactory;
    GroupedDatasetBuilder4(String name,
                           GroupedDataset<IN, KIN> input,
                           UnaryFunction<KIN, KEY> keyExtractor,
                           UnaryFunction<KIN, VALUE> valueExtractor,
                           StateFactory<OUT, STATE> stateFactory)
    {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
      this.valueExtractor = Objects.requireNonNull(valueExtractor);
      this.stateFactory = Objects.requireNonNull(stateFactory);
    }
    public GroupedDatasetBuilder5<IN, KIN, KEY, VALUE, OUT, STATE> combineStateBy(
            CombinableReduceFunction<STATE> stateCombiner) {
      return new GroupedDatasetBuilder5<>(
          name, input, keyExtractor, valueExtractor,
          stateFactory, stateCombiner);
    }
  }
  public static class GroupedDatasetBuilder5<
          IN, KIN, KEY, VALUE, OUT, STATE extends State<VALUE, OUT>>
      extends PartitioningBuilder<KEY, GroupedDatasetBuilder5<IN, KIN, KEY, VALUE, OUT, STATE>>
      implements OutputBuilder<Pair<CompositeKey<IN, KEY>, OUT>>
  {
    private final String name;
    private final GroupedDataset<IN, KIN> input;
    private final UnaryFunction<KIN, KEY> keyExtractor;
    private final UnaryFunction<KIN, VALUE> valueExtractor;
    private final StateFactory<OUT, STATE> stateFactory;
    private final CombinableReduceFunction<STATE> stateCombiner;

    GroupedDatasetBuilder5(String name,
                           GroupedDataset<IN, KIN> input,
                           UnaryFunction<KIN, KEY> keyExtractor,
                           UnaryFunction<KIN, VALUE> valueExtractor,
                           StateFactory<OUT, STATE> stateFactory,
                           CombinableReduceFunction<STATE> stateCombiner)
    {
      // define default partitioning
      super(new DefaultPartitioning<>(input.getPartitioning().getNumPartitions()));

      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
      this.valueExtractor = Objects.requireNonNull(valueExtractor);
      this.stateFactory = Objects.requireNonNull(stateFactory);
      this.stateCombiner = Objects.requireNonNull(stateCombiner);
    }
    public <WIN, WLABEL, W extends WindowContext<WLABEL>>
    GroupedDatasetBuilder6<IN, KIN, WIN, KEY, VALUE, OUT, STATE, WLABEL, W>
    windowBy(Windowing<WIN, WLABEL, W> windowing)
    {
      return new GroupedDatasetBuilder6<>(name, input, keyExtractor, valueExtractor,
              stateFactory, stateCombiner, Objects.requireNonNull(windowing), this);
    }

    @Override
    public Dataset<Pair<CompositeKey<IN, KEY>, OUT>> output() {
      return new GroupedDatasetBuilder6<>(name, input, keyExtractor, valueExtractor,
              stateFactory, stateCombiner, null, this)
          .output();
    }
  }

  public static class GroupedDatasetBuilder6<
          IN, KIN, WIN, KEY, VALUE, OUT, STATE extends State<VALUE, OUT>,
          WLABEL, W extends WindowContext<WLABEL>>
      extends PartitioningBuilder<
          KEY, GroupedDatasetBuilder6<IN, KIN, WIN, KEY, VALUE, OUT, STATE, WLABEL, W>>
      implements OutputBuilder<Pair<CompositeKey<IN, KEY>, OUT>>
  {
    private final String name;
    private final GroupedDataset<IN, KIN> input;
    private final UnaryFunction<KIN, KEY> keyExtractor;
    private final UnaryFunction<KIN, VALUE> valueExtractor;
    private final StateFactory<OUT, STATE> stateFactory;
    private final CombinableReduceFunction<STATE> stateCombiner;
    private final Windowing<WIN, WLABEL, W> windowing;

    GroupedDatasetBuilder6(String name,
                           GroupedDataset<IN, KIN> input,
                           UnaryFunction<KIN, KEY> keyExtractor,
                           UnaryFunction<KIN, VALUE> valueExtractor,
                           StateFactory<OUT, STATE> stateFactory,
                           CombinableReduceFunction<STATE> stateCombiner,
                           Windowing<WIN, WLABEL, W> windowing /* optional */,
                           PartitioningBuilder<KEY, ?> partitioning)
    {
      // initialize partitioning
      super(partitioning);

      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
      this.valueExtractor = Objects.requireNonNull(valueExtractor);
      this.stateFactory = Objects.requireNonNull(stateFactory);
      this.stateCombiner = Objects.requireNonNull(stateCombiner);
      this.windowing = windowing;
    }

    @Override
    public Dataset<Pair<CompositeKey<IN, KEY>, OUT>> output() {
      Flow flow = input.getFlow();

      ReduceStateByKey<IN, KIN, WIN, KEY, VALUE,
          CompositeKey<IN, KEY>, OUT, STATE, WLABEL, W>
          reduceStateByKey =
          new ReduceStateByKey<>(name, flow, input, keyExtractor, valueExtractor,
              windowing, stateFactory, stateCombiner, getPartitioning());
      flow.add(reduceStateByKey);

      return reduceStateByKey.output();
    }
  }

  // -------------

  public static <IN> DatasetBuilder1<IN> of(Dataset<IN> input) {
    return new DatasetBuilder1<>("ReduceStateByKey", input);
  }

  public static <IN, KIN> GroupedDatasetBuilder1<IN, KIN> of(GroupedDataset<IN, KIN> input) {
    return new GroupedDatasetBuilder1<>("ReduceStateByKey", input);
  }

  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  private final StateFactory<OUT, STATE> stateFactory;
  private final UnaryFunction<KIN, VALUE> valueExtractor;
  private final CombinableReduceFunction<STATE> stateCombiner;
  private final boolean grouped;
  
  ReduceStateByKey(String name,
                   Flow flow,
                   Dataset<IN> input,
                   UnaryFunction<KIN, KEY> keyExtractor,
                   UnaryFunction<KIN, VALUE> valueExtractor,
                   Windowing<WIN, WLABEL, W> windowing,
                   StateFactory<OUT, STATE> stateFactory,
                   CombinableReduceFunction<STATE> stateCombiner,
                   Partitioning<KEY> partitioning)
  {
    this(name, flow, input, false, keyExtractor, valueExtractor, windowing,
        stateFactory, stateCombiner, partitioning);
  }

  @SuppressWarnings("unchecked")
  ReduceStateByKey(String name,
                   Flow flow,
                   GroupedDataset<IN, KIN> groupedInput,
                   UnaryFunction<KIN, KEY> keyExtractor,
                   UnaryFunction<KIN, VALUE> valueExtractor,
                   Windowing<WIN, WLABEL, W> windowing,
                   StateFactory<OUT, STATE> stateFactory,
                   CombinableReduceFunction<STATE> stateCombiner,
                   Partitioning<KEY> partitioning)
  {
    this(name, flow, (Dataset) groupedInput, true, keyExtractor, valueExtractor,
        windowing, stateFactory, stateCombiner, partitioning);
  }

  ReduceStateByKey(String name,
                   Flow flow,
                   Dataset<IN> input,
                   boolean grouped,
                   UnaryFunction<KIN, KEY> keyExtractor,
                   UnaryFunction<KIN, VALUE> valueExtractor,
                   Windowing<WIN, WLABEL, W> windowing,
                   StateFactory<OUT, STATE> stateFactory,
                   CombinableReduceFunction<STATE> stateCombiner,
                   Partitioning<KEY> partitioning)
  {
    super(name, flow, input, keyExtractor, windowing, partitioning);
    this.stateFactory = stateFactory;
    this.valueExtractor = valueExtractor;
    this.stateCombiner = stateCombiner;
    this.grouped = grouped;
  }

  public StateFactory<OUT, STATE> getStateFactory() {
    return stateFactory;
  }

  public UnaryFunction<KIN, VALUE> getValueExtractor() {
    return valueExtractor;
  }

  public CombinableReduceFunction<STATE> getStateCombiner() {
    return stateCombiner;
  }

  public boolean isGrouped() {
    return grouped;
  }
}
