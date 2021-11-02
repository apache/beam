import 'package:playground/modules/sdk/models/sdk.dart';
import 'package:playground/modules/examples/models/category_model.dart';

class GetListOfExampleResponse {
  final Map<SDK, List<CategoryModel>> categories;

  GetListOfExampleResponse(this.categories);
}