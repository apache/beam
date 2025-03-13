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

import 'package:playground_components/playground_components.dart';

import '../example_descriptor.dart';

/// To test code folding, read-only, and visible sections. Not runnable.
const goExample = ExampleDescriptor(
  //
  '',
  dbPath: '',
  path:
      '/playground/frontend/playground_components_dev/lib/src/examples/go/content/example.go',
  sdk: Sdk.go,

  fullText: '''
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

package main

import (
	"fmt"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

func OutsideOfSections() {
}

// [START show]
func Folded() {
}

func Unfolded1() {
	fmt.Print("editable")// [START unfold1]
	fmt.Print("readonly")// [START readonly1] [END unfold1] [END readonly1]
}

func Unfolded2() {
	fmt.Print("editable")// [START unfold2]
	fmt.Print("readonly")// [START readonly2] [END unfold2] [END readonly2]
}
// [END show]
''',

  croppedFoldedVisibleText: '''

func Folded() {

func Unfolded1() {
  fmt.Print("editable")
  fmt.Print("readonly")
}

func Unfolded2() {
  fmt.Print("editable")
  fmt.Print("readonly")
}

''',
  foldedVisibleText: '''
/*

package main

func OutsideOfSections() {


func Folded() {

func Unfolded1() {
  fmt.Print("editable")
  fmt.Print("readonly")
}

func Unfolded2() {
  fmt.Print("editable")
  fmt.Print("readonly")
}

''',
);
