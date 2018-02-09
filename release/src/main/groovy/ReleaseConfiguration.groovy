/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

class ReleaseConfiguration {

    // Python Release Candidate
    static final String VERSION             = '2.3.0'
    static final String CANDIDATE_URL       = "https://dist.apache.org/repos/dist/dev/beam/${this.VERSION}/"
    static final String SHA1_FILE_NAME      = "apache-beam-${this.VERSION}-python.zip.sha1"
    static final String MD5_FILE_NAME       = "apache-beam-${this.VERSION}-python.zip.md5"
    static final String BEAM_PYTHON_SDK     = "apache-beam-${this.VERSION}-python.zip"
    static final String BEAM_PYTHON_RELEASE = "apache-beam-${this.VERSION}-source-release.zip"


    // Cloud Configurations
    static final String PROJECT_ID            = 'my-first-project-190318'
    static final String BUCKET_NAME           = 'yifan_auto_verification_test_bucket'
    static final String TEMP_DIR              = '/temp'
    static final int NUM_WORKERS              = 1
    static final String GAME_DATA             = '5000_gaming_data.csv'
    static final String WORDCOUNT_OUTPUT      = 'wordcount_direct.txt'
    static final String PUBSUB_TOPIC1         = 'wordstream-python-topic-1'
    static final String PUBSUB_TOPIC2         = 'wordstream-python-topic-2'
    static final String PUBSUB_SUBSCRIPTION   = 'wordstream-python-sub2'
}
