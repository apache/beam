
class InnerJoin(PTransform[PCollection[KV[K, V1]], PCollection[KV[K, KV[V1, V2]]]]):
  """
  PTransform representing an inner join of two collections of KV elements.
  
  @param <K> Type of the key for both collections
  @param <V1> Type of the values for the left collection.
  @param <V2> Type of the values for the right collection.
  """

  def __init__(self, rightCollection: PCollection[KV[K, V2]]):
      self._rightCollection = rightCollection

  def expand(leftCollection: PCollection[KV[K, V1]]):
      """
      checkNotNull(leftCollection);
      checkNotNull(rightCollection);

      final TupleTag<V1> v1Tuple = new TupleTag<>();
      final TupleTag<V2> v2Tuple = new TupleTag<>();

      PCollection<KV<K, CoGbkResult>> coGbkResultCollection =
          KeyedPCollectionTuple.of(v1Tuple, leftCollection)
              .and(v2Tuple, rightCollection)
              .apply("CoGBK", CoGroupByKey.create());

      return coGbkResultCollection
          .apply(
              "Join",
              ParDo.of(
                  new DoFn<KV<K, CoGbkResult>, KV<K, KV<V1, V2>>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      KV<K, CoGbkResult> e = c.element();

                      Iterable<V1> leftValuesIterable = e.getValue().getAll(v1Tuple);
                      Iterable<V2> rightValuesIterable = e.getValue().getAll(v2Tuple);

                      for (V1 leftValue : leftValuesIterable) {
                        for (V2 rightValue : rightValuesIterable) {
                          c.output(KV.of(e.getKey(), KV.of(leftValue, rightValue)));
                        }
                      }
                    }
                  }))
          .setCoder(
              KvCoder.of(
                  ((KvCoder) leftCollection.getCoder()).getKeyCoder(),
                  KvCoder.of(
                      ((KvCoder) leftCollection.getCoder()).getValueCoder(),
                      ((KvCoder) rightCollection.getCoder()).getValueCoder())));
    }
  }
    """

class LeftOuterJoin(PTransform[PCollection[KV[K, V1]], PCollection[KV[K, KV[V1, V2]]]]):
    """
    PTransform representing a left outer join of two collections of KV elements.
    
    @param <K> Type of the key for both collections
    @param <V1> Type of the values for the left collection.
    @param <V2> Type of the values for the right collection.
   """

    def __init__(self, rightCollection: PCollection[KV[K, V2]], nullValue: V2):
      self._rightCollection = rightCollection
      self._nullValue = nullValue

    def expand(leftCollection: PCollection[KV[K, V1]]) -> PCollection[KV[K, KV[V1, V2]]]:
        """
      checkNotNull(leftCollection);
      checkNotNull(rightCollection);
      checkNotNull(nullValue);
      final TupleTag<V1> v1Tuple = new TupleTag<>();
      final TupleTag<V2> v2Tuple = new TupleTag<>();

      PCollection<KV<K, CoGbkResult>> coGbkResultCollection =
          KeyedPCollectionTuple.of(v1Tuple, leftCollection)
              .and(v2Tuple, rightCollection)
              .apply("CoGBK", CoGroupByKey.create());

      return coGbkResultCollection
          .apply(
              "Join",
              ParDo.of(
                  new DoFn<KV<K, CoGbkResult>, KV<K, KV<V1, V2>>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      KV<K, CoGbkResult> e = c.element();

                      Iterable<V1> leftValuesIterable = e.getValue().getAll(v1Tuple);
                      Iterable<V2> rightValuesIterable = e.getValue().getAll(v2Tuple);

                      for (V1 leftValue : leftValuesIterable) {
                        if (rightValuesIterable.iterator().hasNext()) {
                          for (V2 rightValue : rightValuesIterable) {
                            c.output(KV.of(e.getKey(), KV.of(leftValue, rightValue)));
                          }
                        } else {
                          c.output(KV.of(e.getKey(), KV.of(leftValue, nullValue)));
                        }
                      }
                    }
                  }))
          .setCoder(
              KvCoder.of(
                  ((KvCoder) leftCollection.getCoder()).getKeyCoder(),
                  KvCoder.of(
                      ((KvCoder) leftCollection.getCoder()).getValueCoder(),
                      ((KvCoder) rightCollection.getCoder()).getValueCoder())));
    }
        """

class RightOuterJoin(PTransform[PCollection[KV[K, V1]], PCollection[KV[K, KV[V1, V2]]]]):
    """
    PTransform representing a right outer join of two collections of KV elements.

    @param <K> Type of the key for both collections
    @param <V1> Type of the values for the left collection.
    @param <V2> Type of the values for the right collection.
    """

    def __init__(self, rightCollection: PCollection[KV[K, V2]], nullValue: V1):
        self._rightCollection = rightCollection
        self._nullValue = nullValue
    
    def expand(self, leftCollection: PCollection[KV[K, V1]]) -> PCollection[KV[K, KV[V1, V2]]]:
        """
        checkNotNull(leftCollection);
      checkNotNull(rightCollection);
      checkNotNull(nullValue);

      final TupleTag<V1> v1Tuple = new TupleTag<>();
      final TupleTag<V2> v2Tuple = new TupleTag<>();

      PCollection<KV<K, CoGbkResult>> coGbkResultCollection =
          KeyedPCollectionTuple.of(v1Tuple, leftCollection)
              .and(v2Tuple, rightCollection)
              .apply("CoGBK", CoGroupByKey.create());

      return coGbkResultCollection
          .apply(
              "Join",
              ParDo.of(
                  new DoFn<KV<K, CoGbkResult>, KV<K, KV<V1, V2>>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      KV<K, CoGbkResult> e = c.element();

                      Iterable<V1> leftValuesIterable = e.getValue().getAll(v1Tuple);
                      Iterable<V2> rightValuesIterable = e.getValue().getAll(v2Tuple);

                      for (V2 rightValue : rightValuesIterable) {
                        if (leftValuesIterable.iterator().hasNext()) {
                          for (V1 leftValue : leftValuesIterable) {
                            c.output(KV.of(e.getKey(), KV.of(leftValue, rightValue)));
                          }
                        } else {
                          c.output(KV.of(e.getKey(), KV.of(nullValue, rightValue)));
                        }
                      }
                    }
                  }))
          .setCoder(
              KvCoder.of(
                  ((KvCoder) leftCollection.getCoder()).getKeyCoder(),
                  KvCoder.of(
                      ((KvCoder) leftCollection.getCoder()).getValueCoder(),
                      ((KvCoder) rightCollection.getCoder()).getValueCoder())));
    }
        """


class FullOuterJoin(PTransform[PCollection[KV[K, V1]], PCollection[KV[K, KV[V1, V2]]]]):
    """
    PTransform representing a full outer join of two collections of KV elements.
   
    @param <K> Type of the key for both collections
    @param <V1> Type of the values for the left collection.
    @param <V2> Type of the values for the right collection.
    """

    def __init__(self, rightCollection: PCollection[KV[K, V2]], leftNullValue: V1, rightNullValue: V2):
        self._rightCollection = rightCollection
        self._leftNullValue = leftNullValue
        self._rightNullValue = rightNullValue

    def expand(leftCollection: PCollection[KV[K, V1]]) -> PCollection[KV[K, KV[V1, V2]]]:
        """
checkNotNull(leftCollection);
      checkNotNull(rightCollection);
      checkNotNull(leftNullValue);
      checkNotNull(rightNullValue);

      final TupleTag<V1> v1Tuple = new TupleTag<>();
      final TupleTag<V2> v2Tuple = new TupleTag<>();

      PCollection<KV<K, CoGbkResult>> coGbkResultCollection =
          KeyedPCollectionTuple.of(v1Tuple, leftCollection)
              .and(v2Tuple, rightCollection)
              .apply("CoGBK", CoGroupByKey.create());

      return coGbkResultCollection
          .apply(
              "Join",
              ParDo.of(
                  new DoFn<KV<K, CoGbkResult>, KV<K, KV<V1, V2>>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      KV<K, CoGbkResult> e = c.element();

                      Iterable<V1> leftValuesIterable = e.getValue().getAll(v1Tuple);
                      Iterable<V2> rightValuesIterable = e.getValue().getAll(v2Tuple);
                      if (leftValuesIterable.iterator().hasNext()
                          && rightValuesIterable.iterator().hasNext()) {
                        for (V2 rightValue : rightValuesIterable) {
                          for (V1 leftValue : leftValuesIterable) {
                            c.output(KV.of(e.getKey(), KV.of(leftValue, rightValue)));
                          }
                        }
                      } else if (leftValuesIterable.iterator().hasNext()
                          && !rightValuesIterable.iterator().hasNext()) {
                        for (V1 leftValue : leftValuesIterable) {
                          c.output(KV.of(e.getKey(), KV.of(leftValue, rightNullValue)));
                        }
                      } else if (!leftValuesIterable.iterator().hasNext()
                          && rightValuesIterable.iterator().hasNext()) {
                        for (V2 rightValue : rightValuesIterable) {
                          c.output(KV.of(e.getKey(), KV.of(leftNullValue, rightValue)));
                        }
                      }
                    }
                  }))
          .setCoder(
              KvCoder.of(
                  ((KvCoder) leftCollection.getCoder()).getKeyCoder(),
                  KvCoder.of(
                      ((KvCoder) leftCollection.getCoder()).getValueCoder(),
                      ((KvCoder) rightCollection.getCoder()).getValueCoder())));
    }
    """

def innerJoin(leftCollection: PCollection[KV[K, V1]], rightCollection: PCollection[KV[K, V2]],
    name=None) -> PCollection[KV[K, KV[V1, V2]]]:
    """
    Inner join of two collections of KV elements.
   
    @param leftCollection Left side collection to join.
    @param rightCollection Right side collection to join.
    @param <K> Type of the key for both collections
    @param <V1> Type of the values for the left collection.
    @param <V2> Type of the values for the right collection.
    @return A joined collection of KV where Key is the key and value is a KV where Key is of type
        V1 and Value is type V2.
    """
    if name is None:
        name = "InnerJoin"
    return leftCollection.apply(name, InnerJoin(rightCollection))

def leftOuterJoin(
      leftCollection: PCollection[KV[K, V1]],
      rightCollection: PCollection[KV[K, V2]],
      nullValue: V2,
      name=None):
    """
    Left Outer Join of two collections of KV elements.
    
    @param name Name of the PTransform.
    @param leftCollection Left side collection to join.
    @param rightCollection Right side collection to join.
    @param nullValue Value to use as null value when right side do not match left side.
    @param <K> Type of the key for both collections
    @param <V1> Type of the values for the left collection.
    @param <V2> Type of the values for the right collection.
    @return A joined collection of KV where Key is the key and value is a KV where Key is of type
        V1 and Value is type V2. Values that should be null or empty is replaced with nullValue.
    """
    if name is None:
        name = "LeftOuterJoin"
    return leftCollection.apply(name, LeftOuterJoin(rightCollection, nullValue))

def rightOuterJoin(
    leftCollection: PCollection[KV[K, V1]],
    rightCollection: PCollection[KV[K, V2]],
    nullValue: V1,
    name=None) -> PCollection[KV[K, KV[V1, V2]]]:
    """
    Right Outer Join of two collections of KV elements.
   
    @param name Name of the PTransform.
    @param leftCollection Left side collection to join.
    @param rightCollection Right side collection to join.
    @param nullValue Value to use as null value when left side do not match right side.
    @param <K> Type of the key for both collections
    @param <V1> Type of the values for the left collection.
    @param <V2> Type of the values for the right collection.
    @return A joined collection of KV where Key is the key and value is a KV where Key is of type
        V1 and Value is type V2. Values that should be null or empty is replaced with nullValue.
    """
    if name is None:
        name = "RightOuterJoin"
    return leftCollection.apply(name, RightOuterJoin(rightCollection, nullValue))

def fullOuterJoin(
      leftCollection: PCollection[KV[K, V1]],
      rightCollection: PCollection[KV[K, V2]],
      leftNullValue: V1,
      rightNullValue: V2,
      name=None) -> PCollection[KV[K, KV[V1, V2]]]:
   """
   Full Outer Join of two collections of KV elements.
   
   @param name Name of the PTransform.
   @param leftCollection Left side collection to join.
   @param rightCollection Right side collection to join.
   @param leftNullValue Value to use as null value when left side do not match right side.
   @param rightNullValue Value to use as null value when right side do not match right side.
   @param <K> Type of the key for both collections
   @param <V1> Type of the values for the left collection.
   @param <V2> Type of the values for the right collection.
   @return A joined collection of KV where Key is the key and value is a KV where Key is of type
       V1 and Value is type V2. Values that should be null or empty is replaced with
       leftNullValue/rightNullValue.
   """
   if name is None:
       name = "FullOuterJoin"
   return leftCollection.apply(
        name, FullOuterJoin(rightCollection, leftNullValue, rightNullValue))
