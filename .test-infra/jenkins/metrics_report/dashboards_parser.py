#!/usr/bin/env python
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
import json
import os
import re
import unittest


class Dashboard:
    def __init__(self, file):
        self.file = file
        self.uid, self.queries = self.get_dashboard_uid_and_queries(file)
        self.regexes = set(self.parse_query_to_regex(query) for query in self.queries)

    @staticmethod
    def get_dashboard_uid_and_queries(file):
        queries = []
        with open(file, "r") as f:
            data = json.load(f)
            uid = data.get("uid")
            for panel in data.get("panels", []):
                for target in panel.get("targets", []):
                    query = target.get("query")
                    queries.append(query)

        return uid, queries

    @staticmethod
    def parse_query_to_regex(query):
        select_pattern = r"(.*FROM\s)(.*)(\sWHERE.*)"
        match = re.match(select_pattern, query)
        if match:
            from_ = match.group(2)
            without_quotes = re.sub(r"\"", "", from_)
            without_retention_policy = without_quotes
            if re.match(r"(\w+.\.)(.*)", without_quotes):
                without_retention_policy = re.sub(r"(\w+.)(.*)", r"\2", without_quotes)

            replaced_parameters = re.sub(
                r"\$\{\w+\}", r"[\\w\\d]*", without_retention_policy
            )
            return replaced_parameters

    @staticmethod
    def _get_json_files_from_directory(directory):
        return [
            os.path.join(directory, i)
            for i in os.listdir(directory)
            if i.endswith(".json")
        ]

    @classmethod
    def get_dashboards_from_directory(cls, directory):
        for file in cls._get_json_files_from_directory(directory):
            yield cls(file)


def guess_dashboard_by_measurement(
    measurement, directory, additional_query_substrings=None
):
    """
    Guesses dashboard by measurement name by parsing queries and matching it with measurement.
    It is done by using regular expressions obtained from queries.
    Additionally query can be checked for presence of any of the substrings.
    """
    dashboards = list(Dashboard.get_dashboards_from_directory(directory))
    ret = []
    for dashboard in dashboards:
        for regex in dashboard.regexes:
            if additional_query_substrings and not any(
                substring.lower() in query.lower()
                for substring in additional_query_substrings
                for query in dashboard.queries
            ):
                continue
            if regex and re.match(regex, measurement):
                ret.append(dashboard)
    return list(set(ret))


class TestParseQueryToRegex(unittest.TestCase):
    def test_parse_query_to_regex_1(self):
        query = (
            'SELECT "runtimeMs" FROM "forever"."nexmark_${ID}_${processingType}" WHERE '
            '"runner" =~ /^$runner$/ AND $timeFilter GROUP BY "runner"'
        )
        expected = r"nexmark_[\w\d]*_[\w\d]*"
        result = Dashboard.parse_query_to_regex(query)
        self.assertEqual(expected, result)

    def test_parse_query_to_regex_2(self):
        query = (
            'SELECT mean("value") FROM "python_bqio_read_10GB_results" WHERE "metric" '
            '=~ /runtime/ AND $timeFilter GROUP BY time($__interval), "metric"'
        )
        expected = "python_bqio_read_10GB_results"
        result = Dashboard.parse_query_to_regex(query)
        self.assertEqual(expected, result)

    def test_parse_query_to_regex_3(self):
        query = (
            'SELECT mean("value") FROM "${sdk}_${processingType}_cogbk_3" WHERE '
            '"metric" =~ /runtime/ AND $timeFilter GROUP BY time($__interval), "metric"'
        )
        expected = "[\w\d]*_[\w\d]*_cogbk_3"
        result = Dashboard.parse_query_to_regex(query)
        self.assertEqual(expected, result)
