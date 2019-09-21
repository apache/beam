from driver_meta import DriverMeta

class DriverRow:

    def __init__(self, tuple, enriched=False):
        """
        :param tuple: rdd tuple
        :param enriched: flag, that last column is enriched by kafka ranges
        """
        self.tuple = tuple
        self.enriched = enriched


    def with_kafka_ranges(self, kafka_streaming_ranges):
        """
        :param kafka_streaming_ranges: KafkaUtils object
        :return: self
        """
        if not self.enriched:
            self.tuple = (*self.tuple, kafka_streaming_ranges)
            self.enriched = True
        return self


    def list_meta(self, topics_set):
        """
        :param topics_set: kafka topics as filter
        :return: topic and partition pairs from last column
        """
        if self.enriched:
            result = []
            for range in (self.tuple[-1] if len(self.tuple) > 1 and self.tuple[-1] is not None else []):
                if range.topic in topics_set: result.append(DriverMeta(range.topic, range.partition))
            return result
        raise Exception("list_meta internal error")


    def get_offset(self, meta):
        """
        :param meta: topic and partition as filter
        :return: kafka offset from KafkaUtils object
        """
        if self.enriched:
            for range in (self.tuple[-1] if len(self.tuple) > 1 and self.tuple[-1] is not None else []):
                if meta.topic == range.topic and meta.partition == range.partition: return range.untilOffset
            raise Exception("get_offset not found for meta.topic `" + meta.topic + "' meta.partition `" + str(meta.partition) + "'")
        raise Exception("get_offset internal error")


    def as_row(self):
        """
        :return: spark object (rdd row)
        """
        return self.tuple

