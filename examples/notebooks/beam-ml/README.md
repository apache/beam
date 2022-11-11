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

Starting with the Apache Beam SDK version 2.40, users have access to a
[RunInference](https://beam.apache.org/releases/pydoc/current/apache_beam.ml.inference.base.html#apache_beam.ml.inference.base.RunInference)
transform.

This transform allows you to make inferences or predictions on data for
popular machine learning frameworks like TensorFlow, PyTorch, and
scikit-learn.

## Using The Notebooks

These notebooks illustrate ways to use Apache Beam's RunInference transforms, as well as different
use cases for [ModelHandler](https://beam.apache.org/releases/pydoc/current/apache_beam.ml.inference.base.html#apache_beam.ml.inference.base.ModelHandler) implementations.
Beam comes with multiple ModelHandler implementations.

### Loading the Notebooks

1. To get started quickly with notebooks, use [Colab](https://colab.sandbox.google.com/).
2. In Colab, open the notebook from GitHub using the notebook URL, for example:
```
https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_tensorflow.ipynb
```

3. To run most notebooks, you need to change the Google Cloud project and bucket
to your project and bucket.
