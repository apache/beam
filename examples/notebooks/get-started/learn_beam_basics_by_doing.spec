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
  5:
  - RegexMatch: 'ham	Go until jurong point, crazy\.\. Available only in bugis n great world la e buffet\.\.\. Cine there got amore wat\.\.\. ham	Ok lar\.\.\. Joking wif u oni\.\.\. spam	Free entry in 2 a wkly comp to win FA Cup final tkts 21st May 2005\. Text FA to 87121 to receive entry question\(std txt rate\)T&C's apply 08452810075over18's ham	U dun say so early hor\.\.\. U c already then say\.\.\. ham	Nah I don't think he goes to usf, he lives around here though spam	FreeMsg Hey there darling it's been 3 week's now and no word back! I'd like some fun you up for it still\? Tb ok! XxX std chgs to send, £1\.50 to rcv ham	Even my brother is not like to speak with me\. They treat me like aids patent\. ham	As per your request 'Melle Melle \(Oru Minnaminunginte Nurungu Vettam\)' has been set as your callertune for all Callers\. Press \*9 to copy your friends Callertune spam	WINNER!! As a valued network customer you have been selected to receivea £900 prize reward! To claim call 09061701461\. Claim code KL341\. Valid 12 hours only\. spam	H'
  13:
  - RegexMatch: '#@title Answer inputs_pattern = 'data/SMSSpamCollection'  pipeline = beam\.Pipeline\(\)  outputs = \(     pipeline       \| 'Take in Dataset' >> beam\.io\.ReadFromText\(inputs_pattern\)       # ADDED       \| 'Write results' >> beam\.io\.WriteToText\("ansoutput1", file_name_suffix = "\.txt"\)       \| 'Print the text file name' >> beam\.Map\(print\) \)  pipeline\.run\(\)  # The file this data is saved to is called "ansoutput1" as seen in the WriteToText transform\. # The command below and the transform input should match\. ! head ansoutput1\*\.txt'
