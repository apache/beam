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

# These values are Dataflow costs for running jobs in us-central1.
# The cost values are found at https://cloud.google.com/dataflow/pricing

VCPU_PER_HR_BATCH = 0.056
VCPU_PER_HR_STREAMING = 0.069
MEM_PER_GB_HR_BATCH = 0.003557
MEM_PER_GB_HR_STREAMING = 0.0035557
PD_PER_GB_HR = 0.000054
PD_SSD_PER_GB_HR = 0.000298
SHUFFLE_PER_GB_BATCH = 0.011
SHUFFLE_PER_GB_STREAMING = 0.018

# GPU Resource Pricing
P100_PER_GPU_PER_HOUR = 1.752
V100_PER_GPU_PER_HOUR = 2.976
T4_PER_GPU_PER_HOUR = 0.42
P4_PER_GPU_PER_HOUR = 0.72
L4_PER_GPU_PER_HOUR = 0.672
A100_40GB_PER_GPU_PER_HOUR = 3.72
A100_80GB_PER_GPU_PER_HOUR = 4.7137
