
from apache_beam.transforms.util import CoGroupByKey
from typing import Any, Generic, Iterable, Iterator, TypeVar
import apache_beam as beam
from apache_beam.typehints import typehints

K = TypeVar("K")
V1 = TypeVar("V1")
V2 = TypeVar("V2")
CoGbkResult = TypeVar("CoGbkResult")

class InnerJoin(beam.PTransform[beam.PCollection[typehints.KV[K, V1]], beam.PCollection[typehints.KV[K, typehints.KV[V1, V2]]]], Generic[K, V1, V2]):
  """
  PTransform representing an inner join of two collections of KV elements.
  
  @param <K> Type of the key for both collections
  @param <V1> Type of the values for the left collection.
  @param <V2> Type of the values for the right collection.
  """

  def __init__(self, rightCollection: beam.PCollection[typehints.KV[K, V2]]):
      self._rightCollection = rightCollection

  def expand(self, leftCollection: beam.PCollection[typehints.KV[K, V1]]):
      if leftCollection is None: raise ValueError()
      if self._rightCollection is None: raise ValueError()

      v1Tuple = TupleTag()
      v2Tuple = TupleTag()

      def innerJoinDo(e: typehints.KV[K, CoGbkResult]) -> Iterator[typehints.KV[K, typehints.KV(V1, V2)]]:
        leftValuesIterable: Iterable[V1] = e[1].getAll(v1Tuple)
        rightValuesIterable: Iterable[V2] = e[1].getAll(v2Tuple)

        for leftValue in leftValuesIterable:
            for rightValue in rightValuesIterable:
              yield (e[0], (leftValue, rightValue))

      coGbkResultCollection: beam.PCollection[typehints.KV[K, CoGbkResult]] = KeyedPCollectionTuple.of(v1Tuple, leftCollection).and(v2Tuple, self._rightCollection).apply("CoGBK", CoGroupByKey);
      return coGbkResultCollection.apply(
          "Join",
          beam.ParDo(innerJoinDo)
      )
      """.setCoder(
              KvCoder.of(
                  ((KvCoder) leftCollection.getCoder()).getKeyCoder(),
                  KvCoder.of(
                      ((KvCoder) leftCollection.getCoder()).getValueCoder(),
                      ((KvCoder) rightCollection.getCoder()).getValueCoder())))
      """

class LeftOuterJoin(beam.PTransform[beam.PCollection[typehints.KV[K, V1]], beam.PCollection[typehints.KV[K, typehints.KV[V1, V2]]]]):
    """
    PTransform representing a left outer join of two collections of KV elements.
    
    @param <K> Type of the key for both collections
    @param <V1> Type of the values for the left collection.
    @param <V2> Type of the values for the right collection.
   """

    def __init__(self, rightCollection: beam.PCollection[typehints.KV[K, V2]], nullValue: V2):
        self._rightCollection = rightCollection
        self._nullValue = nullValue

    def expand(self, leftCollection: beam.PCollection[typehints.KV[K, V1]]) -> beam.PCollection[typehints.KV[K, typehints.KV[V1, V2]]]:
        if leftCollection is None: raise ValueError()
        if self._rightCollection is None: raise ValueError()
        if self._nullValue is None: raise ValueError()
        
        v1Tuple: TupleTag[V1] = TupleTag()
        v2Tuple: TupleTag[V2] = TupleTag()
        def leftOuterJoinDo(e: typehints.KV[K, CoGbkResult]) -> Iterator[typehints.KV[K, Any]]:
          leftValuesIterable: Iterable[V1] = e[1].getAll(v1Tuple)
          rightValuesIterable: Iterable[V2] = e[1].getAll(v2Tuple)

          for leftValue in leftValuesIterable:
            hasNext = False
            for rightValue in rightValuesIterable:
              hasNext = True
              yield e[0], (leftValue, rightValue)
            
            if not hasNext:
              yield e[0], (leftValue, self._nullValue)
            
        coGbkResultCollection: beam.PCollection[typehints.KV[K, CoGbkResult]] = KeyedPCollectionTuple.of(v1Tuple, leftCollection).and(v2Tuple, self._rightCollection).apply("CoGBK", CoGroupByKey.create())
        coGbkResultCollection.apply(
              "Join",
              beam.ParDo(leftOuterJoinDo)
        )
        """
      return 
          .setCoder(
              KvCoder.of(
                  ((KvCoder) leftCollection.getCoder()).getKeyCoder(),
                  KvCoder.of(
                      ((KvCoder) leftCollection.getCoder()).getValueCoder(),
                      ((KvCoder) rightCollection.getCoder()).getValueCoder())));
    }
        """

class RightOuterJoin(beam.PTransform[beam.PCollection[typehints.KV[K, V1]], beam.PCollection[typehints.KV[K, typehints.KV[V1, V2]]]]):
    """
    PTransform representing a right outer join of two collections of KV elements.

    @param <K> Type of the key for both collections
    @param <V1> Type of the values for the left collection.
    @param <V2> Type of the values for the right collection.
    """

    def __init__(self, rightCollection: beam.PCollection[typehints.KV[K, V2]], nullValue: V1):
        self._rightCollection = rightCollection
        self._nullValue = nullValue
    
    def expand(self, leftCollection: beam.PCollection[typehints.KV[K, V1]]) -> beam.PCollection[typehints.KV[K, typehints.KV[V1, V2]]]:
        if leftCollection is None: raise ValueError()
        if self._rightCollection is None: raise ValueError() 
        if self._nullValue is None: raise ValueError()

        v1Tuple: TupleTag[V1] = TupleTag()
        v2Tuple: TupleTag[V2] = TupleTag()
        def rightOuterJoinDo(e: typehints.KV[K, CoGbkResult]) -> Iterator[typehints.KV[K, Any]]:
          leftValuesIterable: Iterable[V1] = e[1].getAll(v1Tuple)
          rightValuesIterable: Iterable[V2] = e[1].getAll(v2Tuple)

          for rightValue in rightValuesIterable:
            hasNext = False
            for leftValue in leftValuesIterable:
              hasNext = True
              yield e[0], (leftValue, rightValue)

            if not hasNext:
              yield e[0], (self._nullValue, rightValue)

        coGbkResultCollection: beam.PCollection[typehints.KV[K, CoGbkResult]] = KeyedPCollectionTuple.of(v1Tuple, leftCollection).and(v2Tuple, self._rightCollection).apply("CoGBK", CoGroupByKey.create())
        coGbkResultCollection.apply(
          "Join",
          beam.ParDo(rightOuterJoinDo)
        )
        """


      return 
          .setCoder(
              KvCoder.of(
                  ((KvCoder) leftCollection.getCoder()).getKeyCoder(),
                  KvCoder.of(
                      ((KvCoder) leftCollection.getCoder()).getValueCoder(),
                      ((KvCoder) rightCollection.getCoder()).getValueCoder())));
    }
        """


class FullOuterJoin(beam.PTransform[beam.PCollection[typehints.KV[K, V1]], beam.PCollection[typehints.KV[K, typehints.KV[V1, V2]]]]):
    """
    PTransform representing a full outer join of two collections of KV elements.
   
    @param <K> Type of the key for both collections
    @param <V1> Type of the values for the left collection.
    @param <V2> Type of the values for the right collection.
    """

    def __init__(self, rightCollection: beam.PCollection[typehints.KV[K, V2]], leftNullValue: V1, rightNullValue: V2):
        self._rightCollection = rightCollection
        self._leftNullValue = leftNullValue
        self._rightNullValue = rightNullValue

    def expand(self, leftCollection: beam.PCollection[typehints.KV[K, V1]]) -> beam.PCollection[typehints.KV[K, typehints.KV[V1, V2]]]:
        if leftCollection is None: raise ValueError()
        if self._rightCollection is None: raise ValueError()
        if self._leftNullValue is None: raise ValueError()
        if self._rightNullValue is None: raise ValueError()

        v1Tuple: TupleTag[V1] = TupleTag()
        v2Tuple: TupleTag[V2] = TupleTag()
        def fullOuterJoinDo(e: typehints.KV[K, CoGbkResult]) -> Iterator[typehints.KV[K, Any]]:
            leftValuesIterable: Iterable[V1] = e[1].getAll(v1Tuple)
            rightValuesIterable: Iterable[V2] = e[1].getAll(v2Tuple)

            leftIter = iter(leftValuesIterable)
            try:
              next(leftIter)
              hasLeft = True
            except StopIteration:
              hasLeft = False
            rightIter = iter(rightValuesIterable)
            try:
              next(rightIter)
              hasRight = True
            except StopIteration:
              hasRight = False
          
            if hasLeft and hasRight:
              for rightValue in rightValuesIterable:
                  for leftValue in leftValuesIterable:
                    yield e[0], (leftValue, rightValue)

            elif hasLeft and not hasRight:
                for leftValue in leftValuesIterable:
                  yield e[0], (leftValue, self._rightNullValue)
            elif not hasLeft and hasRight:
                for rightValue in rightValuesIterable:
                  yield e[0], (self._leftNullValue, rightValue)

        coGbkResultCollection: beam.PCollection[typehints.KV[K, CoGbkResult]] = KeyedPCollectionTuple.of(v1Tuple, leftCollection).and(v2Tuple, self._rightCollection).apply("CoGBK", CoGroupByKey.create())
        coGbkResultCollection.apply(
              "Join",
              beam.ParDo(fullOuterJoinDo)
        )
        
        """

      return 
          .setCoder(
              KvCoder.of(
                  ((KvCoder) leftCollection.getCoder()).getKeyCoder(),
                  KvCoder.of(
                      ((KvCoder) leftCollection.getCoder()).getValueCoder(),
                      ((KvCoder) rightCollection.getCoder()).getValueCoder())));
    }
    """

def innerJoin(leftCollection: beam.PCollection[typehints.KV[K, V1]], rightCollection: beam.PCollection[typehints.KV[K, V2]],
    name=None) -> beam.PCollection[typehints.KV[K, typehints.KV[V1, V2]]]:
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
      leftCollection: beam.PCollection[typehints.KV[K, V1]],
      rightCollection: beam.PCollection[typehints.KV[K, V2]],
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
    leftCollection: beam.PCollection[typehints.KV[K, V1]],
    rightCollection: beam.PCollection[typehints.KV[K, V2]],
    nullValue: V1,
    name=None) -> beam.PCollection[typehints.KV[K, typehints.KV[V1, V2]]]:
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
      leftCollection: beam.PCollection[typehints.KV[K, V1]],
      rightCollection: beam.PCollection[typehints.KV[K, V2]],
      leftNullValue: V1,
      rightNullValue: V2,
      name=None) -> beam.PCollection[typehints.KV[K, typehints.KV[V1, V2]]]:
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
