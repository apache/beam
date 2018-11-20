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

"""A Julia set computing workflow: https://en.wikipedia.org/wiki/Julia_set.

This example has in the juliaset/ folder all the code needed to execute the
workflow. It is organized in this way so that it can be packaged as a Python
package and later installed in the VM workers executing the job. The root
directory for the example contains just a "driver" script to launch the job
and the setup.py file needed to create a package.

The advantages for organizing the code is that large projects will naturally
evolve beyond just one module and you will have to make sure the additional
modules are present in the worker.

In Python Dataflow, using the --setup_file option when submitting a job, will
trigger creating a source distribution (as if running python setup.py sdist) and
then staging the resulting tarball in the staging area. The workers, upon
startup, will install the tarball.

Below is a complete command line for running the juliaset workflow remotely as
an example:

python juliaset_main.py \
  --job_name juliaset-$USER \
  --project YOUR-PROJECT \
  --runner DataflowRunner \
  --setup_file ./setup.py \
  --staging_location gs://YOUR-BUCKET/juliaset/staging \
  --temp_location gs://YOUR-BUCKET/juliaset/temp \
  --coordinate_output gs://YOUR-BUCKET/juliaset/out \
  --grid_size 20

"""

from __future__ import absolute_import

import logging

from apache_beam.examples.complete.juliaset.juliaset import juliaset

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  juliaset.run()
