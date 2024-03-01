package jsonvalues.spec.deserializers.avro;

import java.util.Objects;

/**
 * Exception thrown for errors that occur during the deserialization of JSON values based on specifications.
 * <p>
 * This exception extends {@link RuntimeException} to indicate runtime exceptions specific to deserialization
 * processes.
 * </p>
 */
@SuppressWarnings("serial")
public final class JsSpecDeserializerException extends RuntimeException {

  JsSpecDeserializerException(Throwable e) {
    super(Objects.requireNonNull(e)
                 .getMessage(),
          e);
    setStackTrace(e.getStackTrace());
  }

  JsSpecDeserializerException(String message) {
    super(Objects.requireNonNull(message));
  }

  static JsSpecDeserializerException invalidSpecForRecords() {
    return new JsSpecDeserializerException("To deserialize `JsObj`, the spec must be either a `JsObjSpec` " +
                                           "or `oneOf(JsObjSpec... specs)`");
  }
}
