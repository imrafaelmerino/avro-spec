package jsonvalues.spec;

import java.util.Objects;

/**
 * This exception is thrown when there is an issue while converting Avro data to JSON using jsonvalues.
 */
@SuppressWarnings("serial")
public final class AvroToJsonException extends RuntimeException {

  AvroToJsonException(String message) {
    super(Objects.requireNonNull(message));
  }

  AvroToJsonException(Exception e) {
    super(e.getMessage(),
          e);
    setStackTrace(e.getStackTrace());
  }


}
