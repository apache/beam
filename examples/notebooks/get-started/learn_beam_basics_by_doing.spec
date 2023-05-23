#%%

#@title ###### Licensed to the Apache Software Foundation (ASF), Version 2.0 (the "License")

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

notebook: learn_basics_by_doing.ipynb
tests:
  13:
  - RegexMatch: '#@title Answer inputs_pattern = 'data/SMSSpamCollection'  pipeline = beam\.Pipeline\(\)  outputs = \(     pipeline       \| 'Take in Dataset' >> beam\.io\.ReadFromText\(inputs_pattern\)       # ADDED       \| 'Write results' >> beam\.io\.WriteToText\("ansoutput1", file_name_suffix = "\.txt"\)       \| 'Print the text file name' >> beam\.Map\(print\) \)  pipeline\.run\(\)  # The file this data is saved to is called "ansoutput1" as seen in the WriteToText transform\. # The command below and the transform input should match\. ! head ansoutput1\*\.txt'
