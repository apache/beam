# Copyright 2019 Google LLC. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Setup dependencies for local and cloud deployment."""
# pytype: skip-file

import setuptools

TF_VERSION = '1.14.0'

if __name__ == '__main__':
  setuptools.setup(
      name='beam_chicago_taxi',
      version='0.13.0',
      packages=setuptools.find_packages(),
      install_requires=[
          'jupyter>=1.0,<2',
          'notebook>=5.7.8,<5.8',
          'tensorflow==' + TF_VERSION,
          'tensorflow-data-validation>=0.13.1,<0.14',
          'tensorflow-metadata>=0.13.0,<0.14',
          'tensorflow-model-analysis>=0.13.2,<0.14',
          'tensorflow-serving-api>=1.13.0,<1.14',
          'tensorflow-transform>=0.13.0,<0.14',
      ],
      python_requires='>=2.7,!=3.0.*,!=3.1.*,!=3.2.*,!=3.3.*,!=3.4.*,<4',
  )
