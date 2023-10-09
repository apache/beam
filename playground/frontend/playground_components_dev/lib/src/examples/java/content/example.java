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

package main;

import a;
import b;

void OutsideOfSections() {
}

// [START show]
void Folded() {
}

void Unfolded1() {
  System.out.println("editable")// [START unfold1]
  System.out.println("readonly")// [START readonly1] [END unfold1] [END readonly1]
}

void Unfolded2() {
  System.out.println("editable")// [START unfold2]
  System.out.println("readonly")// [START readonly2] [END unfold2] [END readonly2]
}
// [END show]
