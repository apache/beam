package org.beam.dsls.sql.schema;

public class InvalidFieldException extends RuntimeException {

  public InvalidFieldException() {
    super();
  }

  public InvalidFieldException(String message) {
    super(message);
  }

}
