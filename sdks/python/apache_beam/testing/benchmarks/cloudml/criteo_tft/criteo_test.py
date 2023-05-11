@pytest.mark.uses_tft
class FillInMissingTest(unittest.TestCase):
  def test_fill_in_missing(self):
    # Create a rank 2 sparse tensor with missing values
    indices = np.array([[0, 0], [0, 2], [1, 1], [2, 0]])
    values = np.array([1, 2, 3, 4])
    dense_shape = np.array([3, 3])
    sparse_tensor = tf.sparse.SparseTensor(indices, values, dense_shape)

    # Fill in missing values with -1
    filled_tensor = fill_in_missing(sparse_tensor, -1)

    # Convert to a dense tensor and check the values
    expected_output = np.array([1, -1, 2, -1, -1, -1, 4, -1, -1])
    actual_output = filled_tensor.numpy()
    self.assertEqual(expected_output, actual_output)
