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

const getListOfExamplesResponse = {
  'java': [
    {
      'name': 'WordCountCategory',
      'examples': [
        {
          'name': 'MinimalWordCount',
          'id': 1111,
          'type': 'example',
          'description': 'ABCCDEFG',
        },
        {
          'name': 'WordCount',
          'id': 2222,
          'type': 'kata',
          'description': 'ABCCDEFG',
        },
      ],
    },
    {
      'name': 'Kafka',
      'examples': [
        {
          'name': 'KafkaToPubSub',
          'id': 3333,
          'type': 'test',
          'description': 'ABCCDEFG',
        },
      ],
    },
  ],
  'python': [
    {
      'name': 'WordCountCategory',
      'examples': [
        {
          'name': 'MinimalWordCount',
          'id': 4444,
          'type': 'example',
          'description': 'ABCCDEFG',
        },
      ],
    },
  ],
  'go': [
    {
      'name': 'WordCountCategory',
      'examples': [
        {
          'name': 'MinimalWordCount',
          'id': 5555,
          'type': 'example',
          'description': 'ABCCDEFG',
        },
      ],
    },
  ],
};

class ExampleRepository {
  Future<Map<SDK, List<CategoryModel>>> getListOfExamples() async {
    await Future.delayed(const Duration(seconds: 1));
    int responseStatusCode = 200;
    Map<String, List<dynamic>> responseBody = getListOfExamplesResponse;
    switch (responseStatusCode) {
      case 200:
        return parseGetListOfExamplesResponse(responseBody);
      default:
        return {};
    }
  }

  Future<String> getExampleSource(int id) async {
    await Future.delayed(const Duration(milliseconds: 200));
    int responseStatusCode = 200;
    switch (responseStatusCode) {
      case 200:
        return id.toString();
      default:
        return '';
    }
  }

  Map<SDK, List<CategoryModel>> parseGetListOfExamplesResponse(Map data) {
    Map<SDK, List<CategoryModel>> output = {};
    for (SDK sdk in SDK.values) {
      final sdkName = sdk.displayName.toLowerCase();
      if (data.containsKey(sdkName)) {
        output[sdk] = data[sdkName]
            .map((category) => CategoryModel.fromJson(category))
            .cast<CategoryModel>()
            .toList();
      }
    }
    return output;
  }
}
