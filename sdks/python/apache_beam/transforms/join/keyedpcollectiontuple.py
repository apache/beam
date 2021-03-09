
from typing import Any, Generic, List, TypeVar
import apache_beam as beam
from apache_beam.typehints import typehints
from apache_beam.coders.coders import Coder

K = TypeVar("K")
V = TypeVar("V")
InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")

class KeyedPCollectionTuple(Generic[K]):
    """
    An immutable tuple of keyed {@link PCollection PCollections} with key type K. ({@link PCollection
    PCollections} containing values of type {@code KV<K, ?>})
    @param <K> the type of key shared by all constituent PCollections
    """
    def __init__(self, pipeline: beam.Pipeline,
      keyedCollections=None,
      tupleTagList=None,
      keyCoder=None):
        self._pipeline = pipeline
        if keyedCollections is None:
            keyedCollections = []
        # We use a List to properly track the order in which collections are added.
        self._keyedCollections: List[TaggedKeyedPCollection[K, Any]] = keyedCollections
        if tupleTagList is None:
            tupleTagList = TupleTagList.empty()
        self._schema = CoGbkResultSchema(tupleTagList)
        self._keyCoder: Coder = keyCoder

    def getPipeline(self) -> beam.Pipeline:
        return self._pipeline

    def getKeyCoder(self) -> Coder:
        if self._keyCoder is None:
          raise ValueError("cannot return null keyCoder")
        return self._keyCoder

    pipeline = property(getPipeline, doc="Pipeline this Tuple belongs to")
    keyCoder = property(getKeyCoder, doc="Key {@link Coder} for all {@link PCollection PCollections} in this {@link KeyedPCollectionTuple}")

    @staticmethod
    def empty(pipeline: beam.Pipeline):
        """
        Returns an empty {@code KeyedPCollectionTuple<K>} on the given pipeline.
        """
        return KeyedPCollectionTuple(pipeline)

    @staticmethod
    def of(
      tag, pc: beam.PCollection[typehints.KV[K, InputT]]):
        """
        A version of {@link #of(TupleTag, PCollection)} that takes in a string instead of a TupleTag.
        <p>This method is simpler for cases when a typed tuple-tag is not needed to extract a
        PCollection, for example when using schema transforms.
        """
        if tag is str:
            tag = TupleTag(tag)
        return KeyedPCollectionTuple(pc.pipeline).and(tag, pc);

    def and(self, tag: TupleTag[V], pc: beam.PCollection[typehints.KV[K, V]]):
        """
        Returns a new {@code KeyedPCollectionTuple<K>} that is the same as this, appended with the
        given PCollection.
        """
        if pc.pipeline != self.pipeline:
            raise ValueError("PCollections come from different Pipelines")

        wrapper: TaggedKeyedPCollection[K, Any] = TaggedKeyedPCollection(tag, pc)
        myKeyCoder: Coder = getKeyCoder(pc) if self.keyCoder is None else self.keyCoder
        newKeyedCollections: List[TaggedKeyedPCollection[K, Any]] = self._keyedCollections.copy().add(wrapper)
        return KeyedPCollectionTuple(self.pipeline, newKeyedCollections, self._schema.getTupleTagList().and(tag), myKeyCoder)

    def apply(self, transform: beam.PTransform[Any, OutputT], name=None) -> OutputT:
        """
        Applies the given {@link PTransform} to this input {@code KeyedPCollectionTuple} and returns
        its {@code OutputT}. This uses {@code name} to identify the specific application of the
        transform. This name is used in various places, including the monitoring UI, logging, and to
        stably identify this application node in the job graph.
        """
        if name is None:
          return self.pipeline.apply(transform)
        else:
          return self.pipeline.apply(transform, label=name)


def getKeyCoder(pc: beam.PCollection[typehints.KV[K, V]]) -> Coder:
    """
    TODO: This should already have run coder inference for output, but may not have been consumed
    as input yet (and won't be fully specified); This is fine
    """

    """    
    # Assumes that the PCollection uses a KvCoder.
    entryCoder: Coder = pc.getCoder()
if (!(entryCoder instanceof KvCoder<?, ?>)) {
  throw new IllegalArgumentException("PCollection does not use a KvCoder");
}
    @SuppressWarnings("unchecked")
    KvCoder<K, V> coder = (KvCoder<K, V>) entryCoder;
    return coder.getKeyCoder();
    """
    raise NotImplementedError()

"""
public class KeyedPCollectionTuple<K> implements PInput {

  public boolean isEmpty() {
    return keyedCollections.isEmpty();
  }

  /**
   * Returns a list of {@link TaggedKeyedPCollection TaggedKeyedPCollections} for the {@link
   * PCollection PCollections} contained in this {@link KeyedPCollectionTuple}.
   */
  public List<TaggedKeyedPCollection<K, ?>> getKeyedCollections() {
    return keyedCollections;
  }



  /**
   * Expands the component {@link PCollection PCollections}, stripping off any tag-specific
   * information.
   */
  @Override
  public Map<TupleTag<?>, PValue> expand() {
    ImmutableMap.Builder<TupleTag<?>, PValue> retval = ImmutableMap.builder();
    for (TaggedKeyedPCollection<K, ?> taggedPCollection : keyedCollections) {
      retval.put(taggedPCollection.tupleTag, taggedPCollection.pCollection);
    }
    return retval.build();
  }



  /** Returns the {@link CoGbkResultSchema} associated with this {@link KeyedPCollectionTuple}. */
  public CoGbkResultSchema getCoGbkResultSchema() {
    return schema;
  }


"""

class TaggedKeyedPCollection(Generic[K, V]):
    """
    A utility class to help ensure coherence of tag and input PCollection types.
    """

    def __init__(self, tupleTag, pCollection: beam.PCollection[typehints.KV[K, V]]):
      self._tupleTag: TupleTag[V] = tupleTag
      self._pCollection = pCollection

    def getCollection(self):
      return self._pCollection

    def getTupleTag(self):
      return self._tupleTag

    collection = property(getCollection, doc="Underlying PCollection of this TaggedKeyedPCollection")
    tupleTag = property(getTupleTag, doc="TupleTag of this TaggedKeyedPCollection")
