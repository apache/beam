/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.cdap.github.batch;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/** A no-op split. */
public class GithubSplit extends InputSplit implements Writable {

  private String link;

  public GithubSplit() {
    // For serialization
  }

  public GithubSplit(String link) {
    this.link = link;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(link);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.link = dataInput.readUTF();
  }

  @Override
  public long getLength() {
    return 0;
  }

  @Override
  public String[] getLocations() {
    return new String[0];
  }

  public String getLink() {
    return link;
  }
}
