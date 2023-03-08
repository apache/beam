// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bigquery

import (
	"fmt"
	"os/exec"
	"time"
)

// newTempTable creates a new BigQuery table using BigQuery's Data Definition Language (DDL) and the
// "bq query" console command. Reference: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language
// The tables created are set to expire after a day.
//
// newTable takes the name of a BigQuery dataset, the prefix for naming the table, and a DDL schema
// for the data, and generates that table with a unique suffix and an expiration time of a day
// later.
func newTempTable(dataset, prefix, schema string) (string, error) {
	name := fmt.Sprintf("%s.%s_temp_%v", dataset, prefix, time.Now().UnixNano())
	query := fmt.Sprintf("CREATE TABLE `%s`(%s) OPTIONS(expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY))", name, schema)
	cmd := exec.Command("bq", "query", "--use_legacy_sql=false", query)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("creating table through command \"%s\" failed with output:\n%s", cmd.String(), out)
	}
	return name, nil
}

// deleteTable deletes a BigQuery table using BigQuery's Data Definition Language (DDL) and the
// "bq query" console command. Reference: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language
func deleteTempTable(table string) error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table)
	cmd := exec.Command("bq", "query", "--use_legacy_sql=false", query)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("deleting table through command \"%s\" failed with output:\n%s", cmd.String(), out)
	}
	return nil
}
