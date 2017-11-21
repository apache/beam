


class LexicographicPosition(object):
  KEYSPACE_BEGINNING = 1
  KEYSPACE_END = 2


def lexicographic_cmp(left, right):
  if left == LexicographicPosition.KEYSPACE_BEGINNING:
    return -1
  if right == LexicographicPosition.KEYSPACE_END:
    return 1
  return cmp(left, right)


class LexicographicRange(object):
  def __init__(self, start, end):
    assert isinstance(start, str) or start == LexicographicPosition.KEYSPACE_BEGINNING
    assert isinstance(end, str) or end == LexicographicPosition.KEYSPACE_END
    self.start = start
    self.end = end