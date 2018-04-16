#!/usr/bin/env python
#
#
#    Licensed to the Apache Software Foundation (ASF) under one or more
#    contributor license agreements.  See the NOTICE file distributed with
#    this work for additional information regarding copyright ownership.
#    The ASF licenses this file to You under the Apache License, Version 2.0
#    (the "License"); you may not use this file except in compliance with
#    the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#   This script performs basic analytic of performance tests results.
#   It operates in two modes:
#   --mode=report - In this mode script iterates over list of BigQuery tables and
#   analyses the data. This mode is intended to be run on a regulars basis, e.g. daily.
#   Report will contain average tests execution time of given metric, its comparison with
#   with average calculated from historical data, recent standard deviation and standard
#   deviation calculated based on historical data.
#   --mode=validation - In this mode script will analyse single BigQuery table and check
#   recent results.
#
#   Other parameters are described in script. Notification is optional parameter.
#   --send_notification - if present, script will send notification to slack channel.
#   Requires setting env variable SLACK_WEBOOK_URL which value could be obtained by
#   creating incoming webhook on Slack.
#
#   This script is intended to be used only by Jenkins.
#   Example script usage:
#   verify_performance_test_results.py \
#     --bqtable='["beam_performance.avroioit_hdfs_pkb_results", \
#                 "beam_performance.textioit_pkb_results"]' \
#     --metric="run_time" --mode=report --send_notification
#

import argparse, time, calendar, json, re, os, requests
from google.cloud import bigquery

### TIME SETTINGS ###########
TIME_PATTERN = '%d-%m-%Y_%H-%M-%S'
NOW = int(time.time())
# First analysis time interval definition - 24h before
TIME_POINT_1 = NOW - 1 * 86400
# Second analysis time interval definition - week before
TIME_POINT_2 = NOW - 7 * 86400
##############################

SLACK_USER = os.getenv('SLACK_USER', "jenkins-beam")
SLACK_WEBOOK_URL = os.getenv('SLACK_WEBOOK_URL')
SLACK_CHANNEL = os.getenv('SLACK_CHANNEL', "beam-testing")

def submit_big_query_job(sql_command, return_type):
    query_job = client.query(sql_command)
    results = query_job.result()
    if return_type == "list":
        # Queries that have multiple elements in output displayed as query_result
        result_list = []
        for row in results:
            result_list.append(row.query_result)
        return result_list
    elif return_type == "value":
        # All queries must have single element in output displayed as query_result
        for row in results:
            return (row.query_result)
    else:
        print("This type is not supported")
        return None

def count_queries(table_name, time_start, time_stop, metric):
    # This function checks how many data was inserted in time interval.
    sql_command = 'select count(*) as query_result from {} where TIMESTAMP > {} and TIMESTAMP < {} and METRIC=\'{}\''.format(
        table_name,
        time_start,
        time_stop,
        metric
        )
    count = submit_big_query_job(sql_command, "value")
    print("Number of records inserted into {} between {} - {}: {}".format(
        table_name,
        time.strftime(TIME_PATTERN, time.gmtime(time_start)),
        time.strftime(TIME_PATTERN, time.gmtime(time_stop)),
        count))
    return count

def get_average_from(table_name, time_start, time_stop, metric):
    # This function return average value of the provided metric in time interval.
    sql_command = 'select avg(value) as query_result from {} where TIMESTAMP > {} and TIMESTAMP < {} and METRIC=\'{}\''.format(
        table_name,
        time_start,
        time_stop,
        metric
    )
    average = submit_big_query_job(sql_command, "value")
    return average

def get_stddev_from(table_name, time_start, time_stop, metric):
    # This function return standard deviation of the provided metric in time interval.
    sql_command = 'select stddev(value) as query_result from {} where TIMESTAMP > {} and TIMESTAMP < {} and METRIC=\'{}\''.format(
        table_name,
        time_start,
        time_stop,
        metric
    )
    stddev = submit_big_query_job(sql_command, "value")
    return stddev

def get_records_from(table_name, time_start, time_stop, metric, number_of_records):
    # This function checks how many data was inserted in time interval.
    sql_command = 'select value as query_result from {} where TIMESTAMP > {} and TIMESTAMP < {} and METRIC=\'{}\' ORDER BY TIMESTAMP DESC LIMIT {}'.format(
        table_name,
        time_start,
        time_stop,
        metric,
        number_of_records
        )
    list_of_records = submit_big_query_job(sql_command, "list")
    return list_of_records

def calculate_historical_data(bq_table_name, TIME_POINT_2, TIME_POINT_1, metric, nb_older_records, average_recent):
    average_old = get_average_from(bq_table_name, TIME_POINT_2, TIME_POINT_1, metric)
    if nb_older_records > 1:
        stddev_old = get_stddev_from(bq_table_name, TIME_POINT_2, TIME_POINT_1, metric)
    else:
        # Standard deviation is 0 when there is only single value.
        stddev_old = 0
    percentage_change = 100*(average_recent - average_old)/average_old
    return percentage_change, stddev_old

def update_messages(bq_table_name, metric, average_recent, percentage_change, stddev_recent, stddev_old,
                    report_message, slack_report_message):
    report_message = report_message + '{} - {}, avg_time {:.2f}s, change {:+.3f}%, stddev {:.2f}, stddev_old {:.2f} \n'.format(
        bq_table_name, metric, average_recent, percentage_change, stddev_recent, stddev_old)
    slack_report_message = slack_report_message + format_slack_message(
        bq_table_name, metric, average_recent, percentage_change, stddev_recent, stddev_old)
    return report_message, slack_report_message

def create_report(bqtables):
    # This function create a report message from tables.
    report_message = ''
    slack_report_message = ''

    for bqtable in bqtables:
        # Get raw table name
        bq_table_name = re.sub(r'\"|\[|\]', '', bqtable).strip()

        # Make sure there was data records inserted
        nb_recent_records = count_queries(bq_table_name, TIME_POINT_1, NOW, metric)
        if nb_recent_records == 0:
            report_message = report_message + '{} there were no tests results uploaded in recent 24h. \n'.format(bq_table_name)
            slack_report_message = slack_report_message + '`{}` there were no tests results uploaded in recent 24h. :bangbang:\n'.format(bq_table_name)
        else:
            average_recent = get_average_from(bq_table_name, TIME_POINT_1, NOW, metric)
            if nb_recent_records > 1:
                stddev_recent = get_stddev_from(bq_table_name, TIME_POINT_1, NOW, metric)
            else:
                # Standard deviation is 0 when there is only single value.
                stddev_recent = 0
            nb_older_records = count_queries(bq_table_name, TIME_POINT_2, TIME_POINT_1, metric)
            if nb_older_records > 0:
                percentage_change, stddev_old = \
                    calculate_historical_data(bq_table_name, TIME_POINT_2, TIME_POINT_1, metric, nb_older_records, average_recent)
                report_message, slack_report_message = \
                    update_messages(bq_table_name, metric, average_recent, percentage_change, stddev_recent, stddev_old, report_message, slack_report_message)
    print(report_message)
    if args.send_notification:
        notify_on_slack(slack_report_message)


def validate_single_performance_test(bqtables):
    # This function validates single test, runs after tests execution
    report_message = ''
    slack_report_message = ''

    for bqtable in bqtables:
        # Get raw table name
        bq_table_name = re.sub(r'\"|\[|\]', '', bqtable).strip()

        nb_recent_records = count_queries(bq_table_name, TIME_POINT_1, NOW, metric)
        average_recent = get_average_from(bq_table_name, TIME_POINT_1, NOW, metric)
        if nb_recent_records > 1:
            stddev_recent = get_stddev_from(bq_table_name, TIME_POINT_1, NOW, metric)
        else:
            stddev_recent = 0
        nb_older_records = count_queries(bq_table_name, TIME_POINT_2, TIME_POINT_1, metric)
        if nb_older_records > 0:
            percentage_change, stddev_old = \
                calculate_historical_data(bq_table_name, TIME_POINT_2, TIME_POINT_1, metric, nb_older_records, average_recent)
            report_message, slack_report_message = \
                update_messages(bq_table_name, metric, average_recent, percentage_change, stddev_recent, stddev_old, report_message, slack_report_message)
    print(report_message)
    if args.send_notification:
        notify_on_slack(slack_report_message)

def format_slack_message(table_name, metric, avg_time, change, stddev, stddev_old):
    # Function that gives additional slack formatting
    # Highlight table name
    table_name_formatted = '`{}`'.format(table_name)
    # Depending on change value bold or highlight and add emoji
    if change is None:
        change_formatted = "NA"
    elif change >= 0:
        change_formatted = '`{:+.3f}%` :-1:'.format(change)
    else:
        change_formatted = '*{:+.3f}%* :+1:'.format(change)

    # Bold if value available
    stddev_formatted = '*{:.2f}*'.format(stddev)
    stddev_old_formatted = '*{:.2f}*'.format(stddev_old)

    return '{} - \"{}\", avg_time {:.2f}s, change {}, stddev {}, stddev_old {} \n'.format(
        table_name_formatted, metric, avg_time, change_formatted, stddev_formatted, stddev_old_formatted)

def notify_on_slack(message):
    # This function sends message on slack channel defined in environment
    data = {
        'text': message,
        'username': SLACK_USER,
        'icon_emoji': ':robot_face:',
        'channel': SLACK_CHANNEL
    }
    response = requests.post(SLACK_WEBOOK_URL, data=json.dumps(
        data), headers={'Content-Type': 'application/json'})
    print('Response: ' + str(response.text))
    print('Response code: ' + str(response.status_code))

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--bqtable", help="List of tables you want to check.")
    parser.add_argument("--metric", help="Metric name you want to validate.")
    parser.add_argument("--mode", help="Script mode: report/validate", default="validate")
    parser.add_argument("--send_notification", help="Send slack notification.", action='store_true')

    args = parser.parse_args()
    client = bigquery.Client()
    bqtables = args.bqtable.split(",")
    metric = args.metric.strip()

    if args.mode == "report":
        create_report(bqtables)
    elif args.mode == "validate":
        validate_single_performance_test(bqtables)
    else:
        print("This mode is not supported yet.")
