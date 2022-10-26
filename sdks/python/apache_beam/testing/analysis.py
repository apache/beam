import numpy as np
import ruptures as rpt

sample_data = np.array([
    44.7,
    41.3,
    42.61,
    42.02,
    42.9,
    42.3,
    44.3,
    43.37,
    43.11,
    44.3131,
    44.72,
    46.13,
    90,
    48.2,
    47.4,
    49.3,
    56.3,
    43.89,
    47.1411,
    51.54,
    46.40,
    45.09
])


def binary_segmentation_algo(data, breakpoints=2):
  model = "l2"
  algo = rpt.Binseg(model=model).fit(data)
  result = algo.predict(breakpoints)
  print(result)


binary_segmentation_algo(data, 2)

# july 24th to August 10th


class ChangePointAnalysis():
  def __init__(self, data):
    self.data = data

  def find_change_point(self):
    pass
