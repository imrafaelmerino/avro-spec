package jsonvalues.spec.serializers.avro;

import java.util.Objects;

/**
 * Exception thrown for errors that occur during the serialization process in {@link JsonSpecSerializer}.
 */
@SuppressWarnings("serial")
public final class JsonSpecSerializerException extends RuntimeException {

  JsonSpecSerializerException(Throwable e) {
    super(Objects.requireNonNull(e)
                 .getMessage(),
          e);
    setStackTrace(e.getStackTrace());
  }

  JsonSpecSerializerException(String message) {
    super(Objects.requireNonNull(message));
  }

  static JsonSpecSerializerException invalidSpecForRecords() {
    return new JsonSpecSerializerException("To serialize `JsObj`, the spec must be either a `JsObjSpec` or " +
                                           "`oneOf(JsObjSpec... specs)`");
  }
}
