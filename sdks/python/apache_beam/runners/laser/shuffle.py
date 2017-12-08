
import json
import threading
import time

from apache_beam.runners.laser.channels import Interface
from apache_beam.runners.laser.channels import remote_method
from apache_beam.runners.laser.lexicographic import lexicographic_cmp
from apache_beam.runners.laser.lexicographic import LexicographicPosition

class SimpleShuffleDataset(object):
  def __init__(self):
    self.finalized = False
    self.items = []
    self.sorted_items = None
    self.uncommitted_items = {}
    self.committed_txns = set()
    self.lock = threading.Lock()

  def put(self, txn_id, key, value):
    self.put_kvs(txn_id, [(key, value)])

  def put_kvs(self, txn_id, kvs):
    print 'PUT KVS', txn_id
    with self.lock:
      if self.finalized:
        raise Exception('Dataset already finalized.')
      if txn_id in self.committed_txns:
        raise Exception('Can\'t write to already committed transaction: %s.' % txn_id)
      if txn_id not in self.uncommitted_items:
        self.uncommitted_items[txn_id] = []
      for key, value in kvs:
        assert isinstance(key, str)
        assert isinstance(value, str)
        self.uncommitted_items[txn_id].append((key, value))


  def commit_transaction(self, txn_id):
    with self.lock:
      if self.finalized:
        raise Exception('Dataset already finalized.')
      if txn_id in self.committed_txns:
        return # TODO
        raise Exception('Transaction already committed: %s.' % txn_id)
      if txn_id in self.uncommitted_items:
        self.items += self.uncommitted_items[txn_id]
        del self.uncommitted_items[txn_id]
      self.committed_txns.add(txn_id)
    print 'DONE COMMIT TRANSACTION', txn_id


  def finalize(self):
    with self.lock:
      self.finalized = True
      self.sorted_items = sorted(self.items, key=lambda kv: kv[0])
      self.cumulative_size = []
      cumulative = 0
      for k, v in self.sorted_items:
        cumulative += len(k) + len(v)
        self.cumulative_size.append(cumulative)
      self.cumulative_size.append(cumulative)

  def _seek(self, lexicographic_position):
    assert self.finalized
    # returns first index >= the lexicographic position.
    # if greater than all values, returns size of items list
    # else, if less-eq than all values, returns 0
    if lexicographic_position == LexicographicPosition.KEYSPACE_BEGINNING:
      return 0
    if lexicographic_position == LexicographicPosition.KEYSPACE_END:
      return len(self.items)

    # Do binary search on items for position.
    # Invariant: lower is always <= lex position, upper is always > lex position.
    lower = -1
    upper = len(self.items)
    while lower + 1 < upper:
      middle = (lower + upper) / 2
      if lexicographic_cmp(self.items[middle][0], lexicographic_position) >= 0:
        # key at middle is >= lex position
        lower, upper = lower, middle
      else:
        # key at middle is < lex position
        lower, upper = middle, upper

    first_index = upper

    # sanity check
    if len(self.items) == 0:
      assert first_index == 0  # TODO: what to do about this special case?
    elif first_index == 0:
      assert lexicographic_cmp(self.items[0][0], lexicographic_position) >= 0
    elif first_index == len(self.items):
      assert lexicographic_cmp(self.items[-1][0], lexicographic_position) < 0
    else:
      assert lexicographic_cmp(self.items[first_index][0], lexicographic_position) >= 0
      assert lexicographic_cmp(self.items[first_index - 1][0], lexicographic_position) < 0

    return first_index


  def summarize_key_range(self, lexicographic_range):
    assert self.finalized
    first_index = self._seek(lexicographic_range.start)
    one_past_last_index = self._seek(lexicographic_range.end)
    # TODO: audit this code again
    return KeyRangeSummary(one_past_last_index - first_index,
        self.cumulative_size[one_past_last_index] - self.cumulative_size[first_index])

  def read(self, lexicographic_range, continuation_token, suggested_max_bytes=8 * 1024 * 1024):
    assert self.finalized
    first_index = self._seek(lexicographic_range.start)
    one_past_last_index = self._seek(lexicographic_range.end)
    i = first_index
    if continuation_token:
      continuation_data = json.loads(continuation_token)
      continuation_index = continuation_data['start']
      assert first_index <= continuation_index < one_past_last_index
      i = continuation_index
    result = []
    last_key = None
    total_bytes_read = 0
    while i < one_past_last_index:
      key, value = self.sorted_items[i]
      if key != last_key:
        result.append((0, key))
        total_bytes_read += len(key)
        last_key = key
      result.append((1, value))
      total_bytes_read += len(value)
      record_size = len(key) + len(value)
      if total_bytes_read == 0 or total_bytes_read + record_size <= suggested_max_bytes:
        i += 1
      else:
        break
    new_continuation_token = None
    if i < one_past_last_index:
      new_continuation_token = json.dumps({'start': i})
    return result, new_continuation_token


class KeyRangeSummary(object):
  def __init__(self, item_count, byte_count):
    self.item_count = item_count
    self.byte_count = byte_count

  def __repr__(self):
    return 'KeyRangeSummary(items: %d, bytes: %d)' % (self.item_count, self.byte_count)

class ShuffleWorkerInterface(Interface):
  @remote_method(int)
  def create_dataset(self, dataset_id):
    raise NotImplementedError()

  @remote_method(int)
  def delete_dataset(self, dataset_id):
    raise NotImplementedError()

  @remote_method(int, int, list)
  def write(self, dataset_id, txn_id, kvs):
    raise NotImplementedError()


  @remote_method(int, object, str, int, returns=object)
  def read(self, dataset_id, lexicographic_range, continuation_token, suggested_max_bytes):
    raise NotImplementedError()

  @remote_method(int, int)
  def commit_transaction(self, dataset_id, txn_id):
    raise NotImplementedError()

  @remote_method(int, object, returns=object)
  def summarize_key_range(self, dataset_id, lexicographic_range):
    raise NotImplementedError()

  @remote_method(int)
  def finalize_dataset(self, dataset_id):
    raise NotImplementedError()



class SimpleShuffleWorker(ShuffleWorkerInterface):
  def __init__(self):
    self.datasets = {}

  # REMOTE METHOD
  def create_dataset(self, dataset_id):
    self.datasets[dataset_id] = SimpleShuffleDataset()

  # REMOTE METHOD
  def delete_dataset(self, dataset_id):
    de; self.datasets[dataset_id]

  # REMOTE METHOD
  def write(self, dataset_id, txn_id, kvs):
    print 'write KVS', txn_id
    self.datasets[dataset_id].put_kvs(txn_id, kvs)

  # REMOTE METHOD
  def read(self, dataset_id, lexicographic_range, continuation_token, suggested_max_bytes):
    start_time = time.time()
    result = self.datasets[dataset_id].read(lexicographic_range, continuation_token,
        suggested_max_bytes=suggested_max_bytes)
    elapsed_time = time.time() - start_time
    print 'SimpleShuffleWorker.read took %fs.' % elapsed_time
    return result


  # REMOTE METHOD
  def commit_transaction(self, dataset_id, txn_id):
    self.datasets[dataset_id].commit_transaction(txn_id)

  # REMOTE METHOD
  def summarize_key_range(self, dataset_id, lexicographic_range):
    return self.datasets[dataset_id].summarize_key_range(lexicographic_range)


  # REMOTE METHOD
  def finalize_dataset(self, dataset_id):
    self.datasets[dataset_id].finalize()


