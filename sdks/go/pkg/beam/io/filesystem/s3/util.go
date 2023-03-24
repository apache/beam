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

package s3

import (
	"errors"
	"fmt"
	"net/url"
)

// parseURI deconstructs the S3 uri in the format 's3://bucket/key' to (bucket, key)
func parseURI(uri string) (string, string, error) {
	parsed, err := url.Parse(uri)
	if err != nil {
		return "", "", err
	}

	if parsed.Scheme != "s3" {
		return "", "", errors.New("scheme must be 's3'")
	}

	bucket := parsed.Host
	if bucket == "" {
		return "", "", errors.New("bucket must not be empty")
	}

	var key string
	if parsed.Path != "" {
		key = parsed.Path[1:]
	}

	return bucket, key, nil
}

// makeURI constructs an S3 uri from the bucket and key to the format 's3://bucket/key'
func makeURI(bucket string, key string) string {
	return fmt.Sprintf("s3://%s/%s", bucket, key)
}
