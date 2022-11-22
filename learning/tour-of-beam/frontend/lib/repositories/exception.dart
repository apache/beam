abstract class APIException implements Exception {
  final String message;
  const APIException(this.message);

  @override
  String toString() => message;
}

class APIGenericException extends APIException {
  static const String type = 'API Call Failed';

  const APIGenericException(String message) : super('$type: $message');
}

class APIAuthenticationException extends APIException {
  static const String type = 'Authentication Failed';

  const APIAuthenticationException(String message) : super('$type: $message');
}

class APIInternalServerException extends APIException {
  static const String type = 'Internal Server Error';

  const APIInternalServerException()
      : super('$type: Something is wrong on backend side');
}

class InternalFrontendException extends APIException {
  static const String type = 'Internal Error';

  const InternalFrontendException()
      : super('$type: Something is wrong on frontend side');
}
