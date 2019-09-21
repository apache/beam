import datetime
import json

class SuperInfo(object):

    def __init__(self, body_str, headers=None):
        """
        :param body_str: plain text of object (nifi style)
        :param headers: attributes (nifi style)
        """
        self.ts = str(datetime.datetime.now())
        self.body = body_str
        self.headers = headers if headers is not None else {}

    def __str__(self):
        """
        :return: serialization
        """
        return self.ts + ":" + self.body + ":" + str(self.headers)

class SuperFlow(object):

    def __init__(self):
        """
        """
        self.filedata = {}
        self.info = SuperInfo("")

    def __str__(self):
        """
        :return: serialization
        """
        return json.dumps(self.filedata) + ":" + str(self.info)

    def with_header(self, k, v):
        """
        :param k: headers key
        :param v: headers value
        :return:
        """
        self.info.headers[k] = v
        return self

class FlumeHandler:

    def __init__(self, config, executor_config):
        """
        :param config: dictionary (on driver)
        :param executor_config: object (on executor)
        """
        pass
