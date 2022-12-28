# dependsOn: {{ source('simple_pipeline','test_rows') }}
def process(df):
  return df.groupby('id').sum()