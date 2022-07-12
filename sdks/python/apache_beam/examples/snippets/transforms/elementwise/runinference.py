# coding=utf-8
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# pytype: skip-file


def images(test=None):
  # [START images]
import apache_beam as beam
import torch
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerTensor
from torchvision.models import mobilenet_v2

# Save pretrained weights to a local file
model = mobilenet_v2(pretrained=True)
state_dict_path = 'mobilenet_v2_state_dict.pth'
torch.save(model.state_dict(), 'mobilenet_v2_state_dict.pth')

# Load model and state_dict into the model handler
model_class = mobilenet_v2
model_params = {'num_classes': 1000}
model_handler = KeyedModelHandler(
   PytorchModelHandlerTensor(
     state_dict_path=state_dict_path,
     model_class=model_class,
     model_params=model_params))

# Run pipeline
with beam.Pipeline() as p:
    images = (
        p
        | 'CreateImages' >> beam.Create([
            (
              'img1',
              torch.Tensor([
                [[-1.5, -1, -.5],[-.5, 0, .5],[.5, 1, 1.5]], 
                [[-1.5, -1, -.5],[-.5, 0, .5],[.5, 1, 1.5]],
                [[-1.5, -1, -.5],[-.5, 0, .5],[.5, 1, 1.5]]
              ])
            ),
        ])
    )
    predictions = (
        images
        | 'PyTorchRunInference' >> RunInference(model_handler)
        | 'Print' >> beam.Map(print))

    # [END images]
    if test:
      test(images)


def digits(test=None):
  # pylint: disable=line-too-long
  # [START digits]
from sklearn import svm
from sklearn import datasets
import pickle

import apache_beam as beam
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.sklearn_inference import ModelFileType
from apache_beam.ml.inference.sklearn_inference import SklearnModelHandlerNumpy

# Train and pickle model
clf = svm.SVC()
X, y = datasets.load_digits(return_X_y=True)
clf.fit(X, y)
model_uri = 'mnist_model.pkl'
with open(model_uri, 'wb') as file:
  pickle.dump(clf, file)

# Load pickled model into the model handler
model_handler = KeyedModelHandler(
    SklearnModelHandlerNumpy(
        model_file_type=ModelFileType.PICKLE,
        model_uri=model_uri))

# Run pipeline
with beam.Pipeline() as p:
    pixels = (
        p
        | 'CreatePixels' >> beam.Create([
            (y[1], X[1]),
            (y[2], X[2]),
        ])
    )
    predictions = (
        pixels
        | 'SklearnRunInference' >> RunInference(model_handler)
        | 'Print' >> beam.Map(print))

    # [END digits]
    # pylint: enable=line-too-long
    if test:
      test(digits)