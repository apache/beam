import 'package:playground/modules/editor/repository/code_repository/code_client/output_response.dart';
import 'package:playground/modules/examples/repositories/models/get_example_request.dart';
import 'package:playground/modules/examples/repositories/models/get_example_response.dart';
import 'package:playground/modules/examples/repositories/models/get_list_of_examples_request.dart';
import 'package:playground/modules/examples/repositories/models/get_list_of_examples_response.dart';

abstract class ExampleClient {
  Future<GetListOfExampleResponse> getListOfExamples(
    GetListOfExamplesRequestWrapper request,
  );

  Future<GetExampleResponse> getExample(GetExampleRequestWrapper request);

  Future<OutputResponse> getExampleOutput(GetExampleRequestWrapper request);
}
