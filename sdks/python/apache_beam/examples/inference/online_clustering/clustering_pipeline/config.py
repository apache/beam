PROJECT_ID = "apache-beam-testing"
# Subscription for PubSub Topic
SUBSCRIPTION_ID = f"projects/{PROJECT_ID}/subscriptions/newsgroup-dataset-subscription"
JOB_NAME = "online-clustering-birch"
NUM_WORKERS = 1


TOKENIZER_NAME = "sentence-transformers/stsb-distilbert-base"
MODEL_STATE_DICT_PATH = f"gs://{PROJECT_ID}-ml-examples/sentence-transformers-stsb-distilbert-base/pytorch_model.bin"
MODEL_CONFIG_PATH = TOKENIZER_NAME
