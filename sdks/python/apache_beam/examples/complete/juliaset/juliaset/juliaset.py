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

We use the quadratic polinomial f(z) = z*z + c, with c = -.62772 +.42193i
"""

from __future__ import absolute_import

import argparse

import apache_beam as beam


def from_pixel(x, y, n):
  """Converts a NxN pixel position to a (-1..1, -1..1) complex number."""
  return complex(2.0 * x / n - 1.0, 2.0 * y / n - 1.0)


def get_julia_set_point_color((x, y), c, n, max_iterations):
  """Given an pixel, convert it into a point in our julia set."""
  z = from_pixel(x, y, n)
  for i in xrange(max_iterations):
    if z.real * z.real + z.imag * z.imag > 2.0:
      break
    z = z * z + c
  return x, y, i  # pylint: disable=undefined-loop-variable


def generate_julia_set_colors(pipeline, c, n, max_iterations):
  """Compute julia set coordinates for each point in our set."""
  def point_set(n):
    for x in range(n):
      for y in range(n):
        yield (x, y)

  julia_set_colors = (pipeline
                      | 'add points' >> beam.Create(point_set(n))
                      | beam.Map(
                          get_julia_set_point_color, c, n, max_iterations))

  return julia_set_colors


def generate_julia_set_visualization(data, n, max_iterations):
  """Generate the pixel matrix for rendering the julia set as an image."""
  import numpy as np  # pylint: disable=wrong-import-order, wrong-import-position
  colors = []
  for r in range(0, 256, 16):
    for g in range(0, 256, 16):
      for b in range(0, 256, 16):
        colors.append((r, g, b))

  xy = np.zeros((n, n, 3), dtype=np.uint8)
  for x, y, iteration in data:
    xy[x, y] = colors[iteration * len(colors) / max_iterations]

  return xy


def save_julia_set_visualization(out_file, image_array):
  """Save the fractal image of our julia set as a png."""
  from matplotlib import pyplot as plt  # pylint: disable=wrong-import-order, wrong-import-position
  plt.imsave(out_file, image_array, format='png')


def run(argv=None):  # pylint: disable=missing-docstring

  parser = argparse.ArgumentParser()
  parser.add_argument('--grid_size',
                      dest='grid_size',
                      default=1000,
                      help='Size of the NxN matrix')
  parser.add_argument(
      '--coordinate_output',
      dest='coordinate_output',
      required=True,
      help='Output file to write the color coordinates of the image to.')
  parser.add_argument('--image_output',
                      dest='image_output',
                      default=None,
                      help='Output file to write the resulting image to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  p = beam.Pipeline(argv=pipeline_args)
  n = int(known_args.grid_size)

  coordinates = generate_julia_set_colors(p, complex(-.62772, .42193), n, 100)

  # Group each coordinate triplet by its x value, then write the coordinates to
  # the output file with an x-coordinate grouping per line.
  # pylint: disable=expression-not-assigned
  (coordinates
   | 'x coord key' >> beam.Map(lambda (x, y, i): (x, (x, y, i)))
   | 'x coord' >> beam.GroupByKey()
   | 'format' >> beam.Map(
       lambda (k, coords): ' '.join('(%s, %s, %s)' % coord for coord in coords))
   | beam.io.Write(beam.io.TextFileSink(known_args.coordinate_output)))
  # pylint: enable=expression-not-assigned
  p.run()

  # Optionally render the image and save it to a file.
  # TODO(silviuc): Add this functionality.
  # if p.options.image_output is not None:
  #  julia_set_image = generate_julia_set_visualization(
  #      file_with_coordinates, n, 100)
  #  save_julia_set_visualization(p.options.image_output, julia_set_image)
