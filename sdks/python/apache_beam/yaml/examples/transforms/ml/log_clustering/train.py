
import pandas as pd
import numpy as np
import pickle
from pyod.models.knn import KNN
from google.cloud import bigquery

# df = pd.read_csv('./Hadoop_2k.log_structured.csv')
# train_dataset = df.to_numpy()

print("DEBUGGING LOGGING: \t Querying vector embeddings from BigQuery...")
client = bigquery.Client()

sql = """
      SELECT *
      FROM `apache-beam-testing.charlesnguyen.test`
      """

df = client.query_and_wait(sql).to_dataframe()
train_dataset = np.stack(df['embedding'].to_numpy())

print(train_dataset, df.columns, train_dataset.shape)
print("DEBUGGING LOGGING: \t Training KNN model...")
my_knn = KNN(
    n_neighbors=2,
    method='largest',
    metric='euclidean',
    contamination=0.1,
)
my_knn.fit(train_dataset)

print("DEBUGGING LOGGING: \t KNN model trained successfully! Saving model...")
knn_pickled_fn = './knn_model.pkl'
with open(knn_pickled_fn, 'wb') as f:
    pickle.dump(my_knn, f)

print("DEBUGGING LOGGING: \t Making inferences..")
inference = my_knn.decision_function(train_dataset)
print(inference)
