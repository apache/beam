class OffsetRestrictionTracker(RestrictionTracker):
  """An `iobase.RestrictionTracker` implementations for an offset range.

  Offset range is represented as OffsetRange.
  """

  def __init__(self, offset_range):
    assert isinstance(offset_range, OffsetRange)
    self._range = offset_range
    self._current_position = None
    self._last_claim_attempt = None

  def current_restriction(self):
    return self._range

  def start_position(self):
    return self._range.start

  def stop_position(self):
    return self._range.stop

  def default_size(self):
    return self._range.size()

  def try_claim(self, position):
    if self._last_claim_attempt and position <= self._last_claim_attempt:
      raise ValueError(
          'Positions claimed should strictly increase. Trying to claim '
          'position %d while last claim attempt was %d.'
          % (position, self._last_claim_attempt))

    self._last_claim_attempt = position
    if position < self._range.start:
      raise ValueError(
          'Position to be claimed cannot be smaller than the start position '
          'of the range. Tried to claim position %r for the range [%r, %r)'
          % (position, self._range.start, self._range.stop))

    if position >= self._range.start and position < self._range.stop:
      self._current_position = position
      return True

    return False


  def deferred_status(self):
    if self._deferred_residual:
      return (self._deferred_residual, self._deferred_watermark)


class OffsetRestrictionTrackerOperator(RestrictionTrackerOperator):
  def __init__(self, restriction_tracker):
    self.restriction_tracker = restriction_tracker
    self._checkpointed = False
    self._deferred_residual = None
    self._current_watermark = None

  def try_split(self, fraction_of_remainder):
    if not self._checkpointed:
      if self.restriction_tracker._current_position is None:
        cur = self.restriction_tracker.start_position() - 1
      else:
        cur = self.restriction_tracker._current_position
      split_point = (
          cur + int(max(1, (self._range.stop - cur) * fraction_of_remainder)))
      if split_point < self.restriction_tracker.stop_position():
        self.restriction_tracker._range, residual_range = self.restriction_tracker._range.split_at(split_point)
        if fraction_of_remainder == 0:
          self._checkpointed = True
        return self.restriction_tracker._range, residual_range

  def defer_remainder(self, watermark=None):
    self._deferred_watermark = watermark
    self._deferred_residual = self.try_split(0)

  def current_progress(self):
    cur = self.restriction_tracker._current_position
    start = self.restriction_tracker.start_position()
    stop = self.restriction_tracker.stop_position()
    if cur is None:
      fraction = 0.0
    elif stop == start:
      # If self._current_position is not None, we must be done.
      fraction = 1.0
    else:
      fraction = (
          float(cur - start)
          / (stop - self.start))
    return RestrictionProgress(fraction=fraction)

  def check_done(self):
    if self.restriction_tracker. _last_claim_attempt < self.restriction_tracker.stop_position() - 1:
      raise ValueError()

class RangeRestrictionProvider(beam.transforms.core.RestrictionProvider):
  """A RestrictionProvider used by ProduceElementSDF and FanoutElementsSDF."""

  def __init__(self, range):
    self.range = restriction_trackers.OffsetRange(0, range)

  def initial_restriction(self, element):
    return self.range

  def create_tracker(self, restriction):
    return OffsetRestrictionTracker(restriction)

  def create_tracker_operator(self, tracker):
    return OffsetRestrictionTrackerOperator(tracker)

  # Disable initial split for getting more dymanic splits.
  def split(self, element, restriction):
    return [restriction]

  def restriction_size(self, element, restriction):
    return restriction.size()

class RestrictionOperationWrapper(object):
  def __init__(self, restriction_tracker, restriction_operator):
    if not isinstance(restriction_tracker, RestrictionTracker):
      raise TypeError('Initialize RestrictionOperationWrapper requires RestrictionTracker.')
    if not isinstance(restriction_operator, RestrictionTrackerOperator):
      raise TypeError('Initialize RestrictionOperationWrapper requires RestrictionTrackerOperator.')
    self._restriction_tracker = restriction_tracker
    self._restraction_operator = restriction_operator
    self._lock = threading.lock()

  def current_restriction(self):
    with self._lock:
      return self._restriction_tracker.current_restriction()

  def try_claim(self, position):
    with self._lock:
      return self._restriction_tracker.try_claim(position)

  def defer_remainder(self, timestamp):
    if timestamp is None:
      timestamp = datetime.timedelta(microseconds=0)
    elif (not isinstance(timestamp, datetime.datetime) and
          not isinstance(timestamp, datetime.timedelta)):
      raise TypeError('The timestamp of deter_remainder() should either be a datetime or timedelta.')
    with self._lock:
      self._restriction_tracker.defer_remainder(timestamp)

  def check_done(self):
    with self._lock:
      return self._restraction_operator.check_done()

  def try_split(self, fraction_of_remainder):
    with self._lock:
      return self._restriction_operator.try_split(fraction_of_remainder)

  # we don't need lock here since at this point, current bundle has been finished.
  def deferred_status(self):
    return self._restraction_operator.deferred_status()