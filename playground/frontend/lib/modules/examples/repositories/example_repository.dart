import 'package:playground/modules/examples/models/example_model.dart';
import 'package:playground/modules/sdk/models/sdk.dart';

const javaHelloWorld = '''class HelloWorld {
  public static void main(String[] args) {
    System.out.println("Hello World!");
  }
}''';

const pythonHelloWorld = "print(‘Hello World’)";

const goHelloWorld = '''package main

import "fmt"

// this is a comment

func main() {
  fmt.Println("Hello World")
}''';

class ExampleRepository {
  Future<List<ExampleModel>> getExamples() {
    return Future.value([
      const ExampleModel(
        {
          SDK.java: javaHelloWorld,
          SDK.go: goHelloWorld,
          SDK.python: pythonHelloWorld,
        },
        "Initial Example",
      )
    ]);
  }
}
