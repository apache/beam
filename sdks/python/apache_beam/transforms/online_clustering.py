from operator import itemgetter
from typing import Optional

import numpy as np
import pandas as pd
from sklearn.cluster import MiniBatchKMeans

import apache_beam as beam
from apache_beam import pvalue

from apache_beam.coders import PickleCoder, VarIntCoder
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.transforms import core
from apache_beam.transforms import ptransform
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec


class SelectLatestModelState(beam.CombineFn):
  """Selects that latest version of a model after training"""
  def create_accumulator(self):
    # create and initialise accumulator
    return None, 0

  def add_input(self, accumulator, element):
    # accumulates each element from input in accumulator
    if element[1] > accumulator[1]:
      return element
    return accumulator

  def merge_accumulators(self, accumulators):
    # Multiple accumulators could be processed in parallel,
    # this function merges them
    return max(accumulators, key=itemgetter(1))

  def extract_output(self, accumulator):
    # Only output the tracker
    return accumulator[0]


class AssignClusterLabels(core.DoFn):
  """Takes a trained model and input data and labels all data instances using the trained model."""
  def process(self, keyed_batch, model, model_id):
    # 1. Remove the temporary assigned key
    _, batch = keyed_batch

    # 2. Calculate cluster predictions
    cluster_labels = model.predict(batch)

    for e, i in zip(batch, cluster_labels):
      yield PredictionResult(example=e, inference=i, model_id=model_id)


class ClusteringAlgorithm(core.DoFn):
  """Abstract class with the interface that clustering algorithms need to follow."""

  MODEL_SPEC = ReadModifyWriteStateSpec("clustering_model", PickleCoder())
  ITERATION_SPEC = ReadModifyWriteStateSpec(
      'training_iterations', VarIntCoder())
  MODEL_ID = 'ClusteringAlgorithm'

  def __init__(self, n_clusters: int, cluster_args: dict):
    super().__init__()
    self.n_clusters = n_clusters
    self.cluster_args = cluster_args

  def process(
      self,
      keyed_batch,
      model_state=core.DoFn.StateParam(MODEL_SPEC),
      iteration_state=core.DoFn.StateParam(ITERATION_SPEC)):
    raise NotImplementedError


class OnlineKMeans(ClusteringAlgorithm):
  """Online K-Means function. Used the MiniBatchKMeans from sklearn
    More information: https://scikit-learn.org/stable/modules/generated/sklearn.cluster.MiniBatchKMeans.html"""
  MODEL_SPEC = ReadModifyWriteStateSpec("clustering_model", PickleCoder())
  ITERATION_SPEC = ReadModifyWriteStateSpec(
      'training_iterations', VarIntCoder())
  MODEL_ID = 'OnlineKmeans'

  def process(
      self,
      keyed_batch,
      model_state=core.DoFn.StateParam(MODEL_SPEC),
      iteration_state=core.DoFn.StateParam(ITERATION_SPEC)):
    # 1. Initialise or load states
    clustering = model_state.read() or MiniBatchKMeans(
        n_clusters=self.n_clusters, **self.cluster_args)
    iteration = iteration_state.read() or 0

    iteration += 1

    # 2. Remove the temporary assigned keys
    _, batch = keyed_batch

    # 3. Calculate cluster centroids
    clustering.partial_fit(batch)

    # 4. Store the training set and model
    model_state.write(clustering)
    iteration_state.write(iteration)

    yield clustering, iteration


class TypeConversion(core.DoFn):
  """Helper function to convert incoming data to numpy arrays that are accepted by sklearn"""
  def process(self, element, *args, **kwargs):
    if isinstance(element, (tuple, list)):
      yield np.array(element)
    elif isinstance(element, np.ndarray):
      yield element
    elif isinstance(element, (pd.DataFrame, pd.Series)):
      yield element.to_numpy()
    else:
      raise ValueError(f"Unsupported type: {type(element)}")


class ClusteringPreprocessing(ptransform.PTransform):
  def __init__(
      self, n_clusters: int, batch_size: int, is_batched: bool = False):
    """ Preprocessing for Clustering Transformation
        The clustering transform expects batches for performance reasons,
        therefore this batches the data and converts it to numpy arrays,
        which are accepted by sklearn. This transform also adds the same key
        to all batches, such that only 1 state is created and updated during
        clustering updates.

          Example Usage::

            pcoll | RunInference(
                        XGBoostModelHandlerNumpy(
                            model_class="XGBoost Model Class",
                            model_state="my_model_state.json")))

          Args:
          n_clusters: number of clusters used by the algorithm
          batch_size: size of the data batches
          is_batched: boolean value that marks if the collection is already batched
           and thus doesn't need to be batched by this transform
          """
    super().__init__()
    self.n_clusters = n_clusters
    self.batch_size = batch_size
    self.is_batched = is_batched

  def expand(self, pcoll):
    pcoll = (
        pcoll
        | "Convert element to numpy arrays" >> beam.ParDo(TypeConversion()))

    if not self.is_batched:
      pcoll = (
          pcoll
          | "Create batches of elements" >> beam.BatchElements(
              min_batch_size=self.n_clusters, max_batch_size=self.batch_size))

    return (pcoll | "Add a key" >> beam.Map(lambda record: (1, record)))


class OnlineClustering(ptransform.PTransform):
  def __init__(
      self,
      clustering_algorithm,
      n_clusters: int,
      cluster_args: dict,
      batch_size: int = 1024,
      is_batched: bool = False):
    """ Clustering transformation itself, it first preprocesses the data,
        then it applies the clustering transformation step by step on each
        of the batches.

          Example Usage::

            pcoll | OnlineClustering(
                        clustering_algorithm=OnlineKMeansClustering
                        batch_size=1024,
                        n_clusters=6
                        cluster_args={}))

          Args:
          clustering_algorithm: Clustering algorithm (DoFn)
          n_clusters: Number of clusters
          cluster_args: Arguments for the sklearn clustering algorithm (check sklearn documentation for more information)
          batch_size: size of the data batches
          is_batched: boolean value that marks if the collection is already batched
           and thus doesn't need to be batched by this transform
          """
    super().__init__()
    self.clustering_algorithm = clustering_algorithm
    self.n_clusters = n_clusters
    self.batch_size = batch_size
    self.cluster_args = cluster_args
    self.is_batched = is_batched

  def expand(self, pcoll):
    data = (
        pcoll
        | ClusteringPreprocessing(
            n_clusters=self.n_clusters,
            batch_size=self.batch_size,
            is_batched=self.is_batched))

    model = (
        data
        | 'Cluster' >> core.ParDo(
            self.clustering_algorithm(
                n_clusters=self.n_clusters, cluster_args=self.cluster_args))
        | 'Select latest model state' >> core.CombineGlobally(
            SelectLatestModelState()))

    return (
        data
        | 'Assign cluster labels' >> core.ParDo(
            AssignClusterLabels(),
            model=pvalue.AsSingleton(model),
            model_id=self.clustering_algorithm.MODEL_ID))
