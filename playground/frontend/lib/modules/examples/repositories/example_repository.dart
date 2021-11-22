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

import 'package:playground/modules/examples/models/category_model.dart';
import 'package:playground/modules/examples/models/example_model.dart';
import 'package:playground/modules/sdk/models/sdk.dart';

const javaHelloWorld = '''class HelloWorld {
  public static void main(String[] args) {
    System.out.println("Hello World!");
  }
}''';

const pythonHelloWorld = 'print(‘Hello World’)';

const goHelloWorld = '''package main

import "fmt"

// this is a comment

func main() {
  fmt.Println("Hello World")
}''';

const scioHelloWorld = ''' 
object Hello {
    def main(args: Array[String]) = {
        println("Hello, world")
    }
}''';

class ExampleRepository {
  List<CategoryModel>? getCategories() {
    return const [
      CategoryModel('Side Inputs', [
        ExampleModel(
          {
            SDK.java: javaHelloWorld,
            SDK.go: goHelloWorld,
            SDK.python: pythonHelloWorld,
            SDK.scio: scioHelloWorld,
          },
          'HelloWorld',
          ExampleType.example,
        ),
        ExampleModel(
          {
            SDK.java: 'JAVA Source code 1',
            SDK.go: 'GO  Source code 1',
            SDK.python: 'PYTHON  Source code 1',
            SDK.scio: 'SCIO  Source code 1',
          },
          'KATA Source code 1',
          ExampleType.kata,
        ),
      ]),
      CategoryModel('Side Outputs', [
        ExampleModel(
          {
            SDK.java: 'JAVA Source code 2',
            SDK.go: 'GO  Source code 2',
            SDK.python: 'PYTHON  Source code 2',
            SDK.scio: 'SCIO  Source code 2',
          },
          'UNIT TEST Source code 2',
          ExampleType.test,
        ),
        ExampleModel(
          {
            SDK.java: 'JAVA Source code 3',
            SDK.go: 'GO  Source code 3',
            SDK.python: 'PYTHON  Source code 3',
            SDK.scio: 'SCIO  Source code 3',
          },
          'EXAMPLE Source code 3',
          ExampleType.example,
        ),
      ]),
      CategoryModel('I/O', [
        ExampleModel(
          {
            SDK.java: 'JAVA Source code 4',
            SDK.go: 'GO  Source code 4',
            SDK.python: 'PYTHON  Source code 4',
            SDK.scio: 'SCIO  Source code 4',
          },
          'KATA Source code 4',
          ExampleType.kata,
        ),
        ExampleModel(
          {
            SDK.java: 'JAVA Source code 5',
            SDK.go: 'GO  Source code 5',
            SDK.python: 'PYTHON  Source code 5',
            SDK.scio: 'SCIO  Source code 5',
          },
          'UNIT TEST Source code 5',
          ExampleType.test,
        ),
      ]),
    ];
  }
}
