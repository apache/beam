import numpy as np
import pandas as pd
from sklearn.cluster import MiniBatchKMeans

import apache_beam as beam

from apache_beam.coders import PickleCoder
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.transforms import core
from apache_beam.transforms import ptransform
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec


class OnlineKMeans(core.DoFn):
    MODEL_SPEC = ReadModifyWriteStateSpec("clustering_model", PickleCoder())
    DATA_SPEC = ReadModifyWriteStateSpec("data_items", PickleCoder())

    def __init__(self, n_clusters: int, cluster_args: dict, *unused_args, **unused_kwargs):
        super().__init__(*unused_args, **unused_kwargs)
        self.n_clusters = n_clusters
        self.cluster_args = cluster_args

    def process(self, keyed_batch, model_state=core.DoFn.StateParam(MODEL_SPEC),
                data_state=core.DoFn.StateParam(DATA_SPEC)):
        # 1. Initialise or load states
        clustering = model_state.read() or MiniBatchKMeans(n_clusters=self.n_clusters, **self.cluster_args)
        dataset = data_state.read() or []

        # 2. Remove the temporary assigned keys
        _, batch = keyed_batch

        dataset.append(batch)

        # 3. Calculate cluster centroids
        clustering.partial_fit(batch)

        # 4. Calculate cluster predictions
        datapoints = np.concatenate(dataset)
        cluster_labels = clustering.predict(datapoints)

        # 5. Store the training set and model
        model_state.write(clustering)
        data_state.write(dataset)

        for e, i in zip(datapoints, cluster_labels):
            yield PredictionResult(example=e, inference=i, model_id='OnlineKMeans')


class TypeConversion(core.DoFn):

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
    def __init__(self, n_clusters: int, batch_size: int):
        super().__init__()
        self.n_clusters = n_clusters
        self.batch_size = batch_size

    def expand(self, pcoll):
        return (
            pcoll
            | "Convert element to numpy arrays" >> beam.ParDo(TypeConversion())
            | "Create batches of elements" >> beam.BatchElements(min_batch_size=self.n_clusters, max_batch_size=self.batch_size)
            | "Add a key" >> beam.Map(lambda record: (1, record))
        )


class OnlineClustering(ptransform.PTransform):
    def __init__(self, clustering_algorithm, n_clusters: int, batch_size):
        super().__init__()
        self.clustering_algorithm = clustering_algorithm
        self.n_clusters = n_clusters
        self.batch_size = batch_size
        self.cluster_args = {}

    def expand(self, pcoll):
        return (
                pcoll
                | ClusteringPreprocessing(n_clusters=self.n_clusters, batch_size=self.batch_size)
                | 'Cluster' >> core.ParDo(self.clustering_algorithm(n_clusters=self.n_clusters, cluster_args=self.cluster_args))
                | beam.Map(print)
        )
