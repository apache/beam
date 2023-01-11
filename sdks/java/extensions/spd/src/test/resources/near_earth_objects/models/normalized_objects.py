# config: {{ config(indexes=false) }}
# dependsOn:  {{ source('nasa','near_earth_objects') }}
import numpy as np

def process(df):
    df = df.drop(['spk_id','full_name','diameter','albedo','diameter_sigma'],axis='columns',inplace=False)
    numerical_cols = df.select_dtypes(include=np.number).columns.tolist()
    df = df.filter(numerical_cols)
    df = (df-df.mean())/df.std()

    return df