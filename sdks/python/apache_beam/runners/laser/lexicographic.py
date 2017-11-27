


class LexicographicPosition(object):
  KEYSPACE_BEGINNING = 1
  KEYSPACE_END = 2


def lexicographic_cmp(left, right):
  if left == LexicographicPosition.KEYSPACE_BEGINNING:
    return -1
  if right == LexicographicPosition.KEYSPACE_END:
    return 1
  return cmp(left, right)


DEFAULT_LEXICOGRAPHIC_PRECISION = 8

def _to_ints(position, precision=DEFAULT_LEXICOGRAPHIC_PRECISION):
  if position == LexicographicPosition.KEYSPACE_BEGINNING:
    return [0] * precision + [0.0]
  if position == LexicographicPosition.KEYSPACE_END:
    return [256] + [0] * (precision - 1) + [0.0]
  result = []
  assert len(position) <= precision # TODO
  for i in range(precision):
    if i < len(position):
      result.append(ord(position[i]))
    else:
      result.append(0)
  result.append(0.0)
  print '_to_ints', result
  return result

def _to_lex_pos(ints, precision=DEFAULT_LEXICOGRAPHIC_PRECISION):
  print '_to_lex_pos', ints
  if ints[precision] > 0:
    resulting_precision = precision
  else:
    resulting_precision = 0
    for i in range(precision):
      if ints[i] != 0:
        resulting_precision = i + 1
  result = ''
  for i in range(resulting_precision):  # TODO: O(n^2)?
    result += chr(ints[i])
  return result

def _weighted_ints_sum(a, b, weight_a, weight_b):
  assert isinstance(weight_a, int) and weight_a >= 0
  assert isinstance(weight_a, int) and  weight_b >= 0
  assert len(a) == len(b)
  precision = len(a) - 1
  total_weight = weight_a + weight_b
  result = []
  carry_num = 0
  carry_denom = total_weight
  for i in range(precision):
    carry_num += weight_a * a[i] + weight_b * b[i]
    place = carry_num / carry_denom
    result.append(place)
    carry_num = 256 * (carry_num % carry_denom)
  result.append(float(carry_num) / float(carry_denom))
  return result


class LexicographicRange(object):
  def __init__(self, start, end):
    assert isinstance(start, str) or start == LexicographicPosition.KEYSPACE_BEGINNING
    assert isinstance(end, str) or end == LexicographicPosition.KEYSPACE_END
    self.start = start
    self.end = end

  def split(self, count, precision=DEFAULT_LEXICOGRAPHIC_PRECISION):
    assert isinstance(count, int) and count >= 1
    start_ints = _to_ints(self.start, precision=precision)
    end_ints = _to_ints(self.end, precision=precision)
    split_points = []
    split_points.append(self.start)
    for i in range(1, count):
      split_points.append(_to_lex_pos(_weighted_ints_sum(start_ints, end_ints, count - i, i)))
    split_points.append(self.end)
    result = []
    for i in range(len(split_points) - 1):
      result.append(LexicographicRange(split_points[i], split_points[i + 1]))
    return result

  def __repr__(self):
    return 'LexicographicRange(%r, %r)' % (self.start, self.end)



if __name__ == '__main__':
  print _to_ints('a')
  print _to_ints(LexicographicPosition.KEYSPACE_END)
  print LexicographicRange('', 'a')
  print LexicographicRange('', 'a').split(10)
  print LexicographicRange(LexicographicPosition.KEYSPACE_BEGINNING, 'a').split(10)