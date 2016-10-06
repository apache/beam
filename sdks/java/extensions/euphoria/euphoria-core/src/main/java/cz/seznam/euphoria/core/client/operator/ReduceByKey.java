package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.GroupedDataset;
import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowContext;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.ReduceFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.util.Pair;

import java.util.Iterator;
import java.util.Objects;

/**
 * Operator performing state-less aggregation by given reduce function.
 *
 * @param <IN> Type of input records
 * @param <KIN> Type of records entering #keyBy and #valueBy methods
 * @param <KEY> Output type of #keyBy method
 * @param <VALUE> Output type of #valueBy method
 * @param <KEYOUT> Type of output key
 * @param <OUT> Type of output value
 */
public class ReduceByKey<
    IN, KIN, KEY, VALUE, KEYOUT, OUT, WLABEL, W extends WindowContext<WLABEL>>
    extends StateAwareWindowWiseSingleInputOperator<
        IN, IN, KIN, KEY, Pair<KEYOUT, OUT>, WLABEL, W,
        ReduceByKey<IN, KIN, KEY, VALUE, KEYOUT, OUT, WLABEL, W>>
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
    public <OUT> DatasetBuilder4<IN, KEY, IN, OUT> reduceBy(ReduceFunction<IN, OUT> reducer) {
      return new DatasetBuilder4<>(name, input, keyExtractor, e-> e, reducer);
    }
    @SuppressWarnings("unchecked")
    public DatasetBuilder4<IN, KEY, IN, IN> combineBy(CombinableReduceFunction<IN> reducer) {
      return new DatasetBuilder4<>(name, input, keyExtractor, e -> e, (ReduceFunction) reducer);
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
    public <OUT> DatasetBuilder4<IN, KEY, VALUE, OUT> reduceBy(
        ReduceFunction<VALUE, OUT> reducer) {
      return new DatasetBuilder4<>(name, input, keyExtractor, valueExtractor, reducer);
    }
    public DatasetBuilder4<IN, KEY, VALUE, VALUE> combineBy(
        CombinableReduceFunction<VALUE> reducer) {
      return new DatasetBuilder4<>(name, input, keyExtractor, valueExtractor, reducer);
    }
  }
  public static class DatasetBuilder4<IN, KEY, VALUE, OUT>
          extends PartitioningBuilder<KEY, DatasetBuilder4<IN, KEY, VALUE, OUT>>
          implements OutputBuilder<Pair<KEY, OUT>>
  {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, KEY> keyExtractor;
    private final UnaryFunction<IN, VALUE> valueExtractor;
    private final ReduceFunction<VALUE, OUT> reducer;
    DatasetBuilder4(String name,
                    Dataset<IN> input,
                    UnaryFunction<IN, KEY> keyExtractor,
                    UnaryFunction<IN, VALUE> valueExtractor,
                    ReduceFunction<VALUE, OUT> reducer)
    {
      // initialize default partitioning according to input
      super(new DefaultPartitioning<>(input.getPartitioning().getNumPartitions()));

      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
      this.valueExtractor = Objects.requireNonNull(valueExtractor);
      this.reducer = Objects.requireNonNull(reducer);
    }
    public  <WLABEL, W extends WindowContext<WLABEL>>
    DatasetBuilder5<IN, KEY, VALUE, OUT, WLABEL, W>
    windowBy(Windowing<IN, WLABEL, W> windowing) {
      return new DatasetBuilder5<>(name, input, keyExtractor, valueExtractor,
              reducer, Objects.requireNonNull(windowing), this);
    }

    @Override
    public Dataset<Pair<KEY, OUT>> output() {
      return new DatasetBuilder5<>(name, input, keyExtractor, valueExtractor,
              reducer, null, this)
          .output();
    }
  }

  public static class DatasetBuilder5<
          IN, KEY, VALUE, OUT, WLABEL, W extends WindowContext<WLABEL>>
      extends PartitioningBuilder<KEY, DatasetBuilder5<IN, KEY, VALUE, OUT, WLABEL, W>>
      implements OutputBuilder<Pair<KEY, OUT>>
  {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, KEY> keyExtractor;
    private final UnaryFunction<IN, VALUE> valueExtractor;
    private final ReduceFunction<VALUE, OUT> reducer;
    private final Windowing<IN, WLABEL, W> windowing;

    DatasetBuilder5(String name,
                    Dataset<IN> input,
                    UnaryFunction<IN, KEY> keyExtractor,
                    UnaryFunction<IN, VALUE> valueExtractor,
                    ReduceFunction<VALUE, OUT> reducer,
                    Windowing<IN, WLABEL, W> windowing /* optional */,
                    PartitioningBuilder<KEY, ?> partitioning)
    {
      // initialize default partitioning according to input
      super(partitioning);

      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
      this.valueExtractor = Objects.requireNonNull(valueExtractor);
      this.reducer = Objects.requireNonNull(reducer);
      this.windowing = windowing;
    }

    @Override
    public Dataset<Pair<KEY, OUT>> output() {
      Flow flow = input.getFlow();
      ReduceByKey<IN, IN, KEY, VALUE, KEY, OUT, WLABEL, W>
          reduce =
          new ReduceByKey<>(name, flow, input, keyExtractor, valueExtractor,
              windowing, reducer, getPartitioning());
      flow.add(reduce);
      return reduce.output();
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
    public <OUT> GroupedDatasetBuilder4<IN, KIN, KEY, KIN, OUT> reduceBy(ReduceFunction<KIN, OUT> reducer) {
      return new GroupedDatasetBuilder4<>(name, input, keyExtractor, e-> e, reducer);
    }
    public GroupedDatasetBuilder4<IN, KIN, KEY, KIN, KIN> combineBy(CombinableReduceFunction<KIN> reducer) {
      return new GroupedDatasetBuilder4<>(name, input, keyExtractor, e -> e, reducer);
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
    public <OUT> GroupedDatasetBuilder4<IN, KIN, KEY, VALUE, OUT> reduceBy(
            ReduceFunction<VALUE, OUT> reducer) {
      return new GroupedDatasetBuilder4<>(name, input, keyExtractor, valueExtractor, reducer);
    }
    public GroupedDatasetBuilder4<IN, KIN, KEY, VALUE, VALUE> combineBy(
            CombinableReduceFunction<VALUE> reducer) {
      return new GroupedDatasetBuilder4<>(name, input, keyExtractor, valueExtractor, reducer);
    }
  }
  public static class GroupedDatasetBuilder4<IN, KIN, KEY, VALUE, OUT>
      extends PartitioningBuilder<KEY, GroupedDatasetBuilder4<IN, KIN, KEY, VALUE, OUT>>
      implements OutputBuilder<Pair<CompositeKey<IN, KEY>, OUT>>
  {
    private final String name;
    private final GroupedDataset<IN, KIN> input;
    private final UnaryFunction<KIN, KEY> keyExtractor;
    private final UnaryFunction<KIN, VALUE> valueExtractor;
    private final ReduceFunction<VALUE, OUT> reducer;
    GroupedDatasetBuilder4(String name,
                           GroupedDataset<IN, KIN> input,
                           UnaryFunction<KIN, KEY> keyExtractor,
                           UnaryFunction<KIN, VALUE> valueExtractor,
                           ReduceFunction<VALUE, OUT> reducer)
    {
      // initialize default partitioning according to input
      super(new DefaultPartitioning<>(input.getPartitioning().getNumPartitions()));

      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
      this.valueExtractor = Objects.requireNonNull(valueExtractor);
      this.reducer = Objects.requireNonNull(reducer);
    }
    public <WLABEL, W extends WindowContext<WLABEL>>
    GroupedDatasetBuilder5<IN, KIN, KEY, VALUE, OUT, WLABEL, W>
    windowBy(Windowing<IN, WLABEL, W> windowing) {
      return new GroupedDatasetBuilder5<>(name, input, keyExtractor, valueExtractor,
              reducer, Objects.requireNonNull(windowing), this);
    }
    @Override
    public Dataset<Pair<CompositeKey<IN, KEY>, OUT>> output() {
      return new GroupedDatasetBuilder5<>(name, input, keyExtractor, valueExtractor,
          reducer, null, this).output();
    }
  }

  public static class GroupedDatasetBuilder5<
          IN, KIN, KEY, VALUE, OUT, WLABEL, W extends WindowContext<WLABEL>>
      extends PartitioningBuilder<
          KEY, GroupedDatasetBuilder5<IN, KIN, KEY, VALUE, OUT, WLABEL, W>>
      implements OutputBuilder<Pair<CompositeKey<IN, KEY>, OUT>>
  {
    private final String name;
    private final GroupedDataset<IN, KIN> input;
    private final UnaryFunction<KIN, KEY> keyExtractor;
    private final UnaryFunction<KIN, VALUE> valueExtractor;
    private final ReduceFunction<VALUE, OUT> reducer;
    private final Windowing<IN, WLABEL, W> windowing;
    GroupedDatasetBuilder5(String name,
                           GroupedDataset<IN, KIN> input,
                           UnaryFunction<KIN, KEY> keyExtractor,
                           UnaryFunction<KIN, VALUE> valueExtractor,
                           ReduceFunction<VALUE, OUT> reducer,
                           Windowing<IN, WLABEL, W> windowing,
                           PartitioningBuilder<KEY, ?> partitioning)
    {
      super(partitioning);

      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
      this.valueExtractor = Objects.requireNonNull(valueExtractor);
      this.reducer = Objects.requireNonNull(reducer);
      this.windowing = windowing;
    }

    @Override
    public Dataset<Pair<CompositeKey<IN, KEY>, OUT>> output() {
      Flow flow = input.getFlow();
      ReduceByKey<IN, KIN, KEY, VALUE, CompositeKey<IN, KEY>, OUT, WLABEL, W>
          reduce =
          new ReduceByKey<>(name, flow, input, keyExtractor, valueExtractor,
              windowing, reducer, getPartitioning());
      flow.add(reduce);
      return reduce.output();
    }
  }

  public static <IN> DatasetBuilder1<IN> of(Dataset<IN> input) {
    return new DatasetBuilder1<>("ReduceByKey", input);
  }

  public static <KEY, IN> GroupedDatasetBuilder1<KEY, IN> of(
      GroupedDataset<KEY, IN> input) {
    return new GroupedDatasetBuilder1<>("ReduceByKey", input);
  }

  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  final ReduceFunction<VALUE, OUT> reducer;
  final UnaryFunction<KIN, VALUE> valueExtractor;
  final boolean grouped;


  ReduceByKey(String name,
              Flow flow,
              Dataset<IN> input,
              UnaryFunction<KIN, KEY> keyExtractor,
              UnaryFunction<KIN, VALUE> valueExtractor,
              Windowing<IN, WLABEL, W> windowing,
              ReduceFunction<VALUE, OUT> reducer,
              Partitioning<KEY> partitioning)
  {
    super(name, flow, input, keyExtractor, windowing, partitioning);
    this.reducer = reducer;
    this.valueExtractor = valueExtractor;
    this.grouped = false;
  }

  @SuppressWarnings("unchecked")
  ReduceByKey(String name,
              Flow flow,
              GroupedDataset<IN, KIN> groupedInput,
              UnaryFunction<KIN, KEY> keyExtractor,
              UnaryFunction<KIN, VALUE> valueExtractor,
              Windowing<IN, WLABEL, W> windowing,
              ReduceFunction<VALUE, OUT> reducer,
              Partitioning<KEY> partitioning)
  {
    super(name, flow, (Dataset) groupedInput, keyExtractor, windowing, partitioning);
    this.reducer = reducer;
    this.valueExtractor = valueExtractor;
    this.grouped = true;
  }
  
  public ReduceFunction<VALUE, OUT> getReducer() {
    return reducer;
  }

  public UnaryFunction<KIN, VALUE> getValueExtractor() {
    return valueExtractor;
  }

  public boolean isGrouped() {
    return this.grouped;
  }

  /**
   * @return {@code TRUE} when combinable reduce function provided
   */
  public boolean isCombinable() {
    return reducer instanceof CombinableReduceFunction;
  }

  // state represents the output value
  private static class ReduceState<VALUE, OUT> extends State<VALUE, OUT> {

    private final ReduceFunction<VALUE, OUT> reducer;
    private final boolean combinable;

    final ListStorage<VALUE> reducableValues;

    ReduceState(Context<OUT> context,
                StorageProvider storageProvider,
                ReduceFunction<VALUE, OUT> reducer,
                boolean combinable)
    {
      super(context, storageProvider);
      this.reducer = Objects.requireNonNull(reducer);
      this.combinable = combinable;
      reducableValues = storageProvider.getListStorage(
          ListStorageDescriptor.of("values", (Class) Object.class));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void add(VALUE element) {
      reducableValues.add(element);
      combineIfPossible();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void flush() {
      OUT result = reducer.apply(reducableValues.get());
      getContext().collect(result);
    }

    void add(ReduceState other) {
      this.reducableValues.addAll(other.reducableValues.get());
      combineIfPossible();
    }

    @SuppressWarnings("unchecked")
    private void combineIfPossible() {
      if (combinable) {
        OUT val = reducer.apply(reducableValues.get());
        reducableValues.clear();
        reducableValues.add((VALUE) val);
      }
    }

    @Override
    public void close() {
      reducableValues.clear();
    }

  }

  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    // this can be implemented using ReduceStateByKey

    Flow flow = getFlow();
    Operator<?, ?> reduceState;
    reduceState = new ReduceStateByKey<>(getName(),
        flow, input, grouped, keyExtractor, valueExtractor,
        windowing,
        (Context<OUT> c, StorageProvider provider) -> new ReduceState<>(
            c, provider, reducer, isCombinable()),
        (Iterable<ReduceState> states) -> {
          final ReduceState first;
          Iterator<ReduceState> i = states.iterator();
          first = i.next();
          while (i.hasNext()) {
            first.add(i.next());
          }
          return first;
        },
        partitioning);
    return DAG.of(reduceState);
  }


}
