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

"""A common file for CloudML benchmarks.

This file contains constants for pipeline paths, dependency locations and
test data paths.
"""

INPUT_CRITEO_SMALL = 'testdata/criteo/train10.tsv'
INPUT_CRITEO_SMALL_100MB = 'testdata/criteo/raw/100mb/train.txt'
INPUT_CRITEO_MEDIUM_1GB = 'testdata/criteo/raw/1gb/train.txt'
INPUT_CRITEO_10GB = 'testdata/criteo/raw/10gb/train.txt'

# The model is trained by running the criteo preprocessing and training.
# The input dataset was the Criteo 10GB dataset and frequency_threshold=100 was
# set for categorical features.
MODEL_CRITEO_10GB = 'testdata/criteo/saved_model'

# The model is trained by running the criteo preprocessing and training.
# The input dataset was the Criteo 10GB dataset and frequency_threshold=100 was
# set for categorical features.
EVAL_MODEL_CRITEO_10GB = 'testdata/criteo/saved_model/tfma'

FREQUENCY_THRESHOLD = '5'
ENABLE_SHUFFLE = True
