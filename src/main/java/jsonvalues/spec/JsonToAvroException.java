package jsonvalues.spec;

import java.util.Objects;
import org.apache.avro.Schema;

/**
 * This exception is thrown when there is an issue while converting JSON using jsonvalues to Avro data.
 */
@SuppressWarnings("serial")
public final class JsonToAvroException extends RuntimeException {

  JsonToAvroException(String message) {
    super(Objects.requireNonNull(message));
  }

  JsonToAvroException(Exception e) {
    super(e.getMessage(),
          e);
    setStackTrace(e.getStackTrace());
  }

  static JsonToAvroException invalidRecordSchema(Schema.Type type) {

    return new JsonToAvroException("The schema `%s` can't be converted into a Record".formatted(type));
  }

  static JsonToAvroException unresolvableUnion(Schema union) {
    return new JsonToAvroException("No schema of the union `%s` conforms the given object".formatted(union));
  }
}
