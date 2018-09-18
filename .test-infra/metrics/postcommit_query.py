#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#

from google.cloud import bigquery
import json

class BeamPostCommitQuery():
  def __init__(self):
    self.client = bigquery.Client(project='apache-beam-testing')

  def loadData(self):
    sql = """
          SELECT job_name,
            SUM(IF(day=0,passes,0)) AS day1_passes,
            SUM(IF(day=0,runs,0)) AS day1_runs,
            SUM(IF(day<7,passes,0)) AS day7_passes,
            SUM(IF(day<7,runs,0)) AS day7_runs,
            SUM(IF(day<28,passes,0)) AS day28_passes,
            SUM(IF(day<28,runs,0)) AS day28_runs
          FROM (SELECT job_name,
            INTEGER(((now()-build_timestamp)/(3600*1000000)) / 24) as day, 
            SUM(build_result="SUCCESS") AS passes,
            COUNT(build_result) AS runs,
          FROM beam_metrics.builds
          WHERE (job_name CONTAINS "PostCommit" OR job_name CONTAINS "Rel" OR
            (job_name CONTAINS "PreCommit" AND job_name CONTAINS "Cron")) AND
            NOT job_name LIKE "%_Phrase" AND
            NOT job_name LIKE "%_PR"
          GROUP BY job_name,day
          HAVING day < 28)
          GROUP BY job_name
          ORDER BY job_name"""
    job_config = bigquery.QueryJobConfig()
    job_config.use_legacy_sql = True
    data = self.client.query(sql,job_config=job_config)
    ret = []
    for rec in list(data):
      val = {'build': rec[0], 'day1_passes': rec[1], 'day1_runs': rec[2],
             'day7_passes': rec[3], 'day7_runs': rec[4],
             'day28_passes': rec[5], 'day28_runs': rec[6]}
      ret.append(val)
    return ret
  
  def getData(self):
    print('Getting data...')
    data = {'data': self.loadData()}
    print('Got data!!.')
    print(data)
    return json.dumps(data, indent=4)
