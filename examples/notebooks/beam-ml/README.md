<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->
# ML Sample Notebooks

As of Beam 2.40 users now have access to a
[RunInference](https://beam.apache.org/releases/pydoc/current/apache_beam.ml.inference.base.html#apache_beam.ml.inference.base.RunInference)
transform.

This allows inferences or predictions of on data for
popular ML frameworks like TensorFlow, PyTorch and
scikit-learn.

## Using The Notebooks

These notebooks illustrate usages of Beam's RunInference, as well as different
usages of implementations of [ModelHandler](https://beam.apache.org/releases/pydoc/current/apache_beam.ml.inference.base.html#apache_beam.ml.inference.base.ModelHandler).
Beam comes with various implementations of ModelHandler.

### Loading the Notebooks

1. A quick way to get started is with [Colab](https://colab.sandbox.google.com/).
2. Load the notebook from github, for example:
```
https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_tensorflow.ipynb.
```

3. To run most notebooks, you will need to change the GCP project and bucket
to your project and bucket.
