#   Licensed to the Apache Software Foundation (ASF) under one
#   or more contributor license agreements.  See the NOTICE file
#   distributed with this work for additional information
#   regarding copyright ownership.  The ASF licenses this file
#   to you under the Apache License, Version 2.0 (the
#   "License"); you may not use this file except in compliance
#   with the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

# beam-playground:
#   name: create-model
#   description: Create models for ml
#   multifile: true
#   files:
#     - name: five_times_table_torch.pt
#     - name: ten_times_table_torch.pt
#   context_line: 49
#   categories:
#     - Quickstart
#   complexity: ADVANCED
#   tags:
#     - hellobeam

import os
import warnings

import numpy
import torch

warnings.filterwarnings('ignore')

# Constants
project = ""
bucket = ""

# To avoid warnings, set the project.
os.environ['GOOGLE_CLOUD_PROJECT'] = project

save_model_dir_multiply_five = 'five_times_table_torch.pt'
save_model_dir_multiply_ten = 'ten_times_table_torch.pt'

class LinearRegression(torch.nn.Module):
    def __init__(self, input_dim=1, output_dim=1):
        super().__init__()
        self.linear = torch.nn.Linear(input_dim, output_dim)

    def forward(self, x):
        out = self.linear(x)
        return out

# Train the linear regression mode on 5 times data
x = numpy.arange(0, 100, dtype=numpy.float32).reshape(-1, 1)
y = (x * 5).reshape(-1, 1)
value_to_predict = numpy.array([20, 40, 60, 90], dtype=numpy.float32).reshape(-1, 1)

five_times_model = LinearRegression()
optimizer = torch.optim.Adam(five_times_model.parameters())
loss_fn = torch.nn.L1Loss()

"""
Train the five_times_model
"""
epochs = 10000
tensor_x = torch.from_numpy(x)
tensor_y = torch.from_numpy(y)
for epoch in range(epochs):
    y_pred = five_times_model(tensor_x)
    loss = loss_fn(y_pred, tensor_y)
    five_times_model.zero_grad()
    loss.backward()
    optimizer.step()

torch.save(five_times_model.state_dict(), save_model_dir_multiply_five)
print(os.path.exists(save_model_dir_multiply_five))  # Verify that the model is saved.

# Prepare train and test data for a 10 times model
x = numpy.arange(0, 100, dtype=numpy.float32).reshape(-1, 1)
y = (x * 10).reshape(-1, 1)

ten_times_model = LinearRegression()
optimizer = torch.optim.Adam(ten_times_model.parameters())
loss_fn = torch.nn.L1Loss()

epochs = 10000
tensor_x = torch.from_numpy(x)
tensor_y = torch.from_numpy(y)
for epoch in range(epochs):
    y_pred = ten_times_model(tensor_x)
    loss = loss_fn(y_pred, tensor_y)
    ten_times_model.zero_grad()
    loss.backward()
    optimizer.step()

torch.save(ten_times_model.state_dict(), save_model_dir_multiply_ten)
print(os.path.exists(save_model_dir_multiply_ten)) # verify if the model is saved