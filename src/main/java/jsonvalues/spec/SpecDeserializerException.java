package jsonvalues.spec;

import java.util.Objects;

/**
 * Exception thrown for errors that occur during the deserialization of JSON values based on specifications.
 * <p>
 * This exception extends {@link RuntimeException} to indicate runtime exceptions specific to deserialization
 * processes.
 * </p>
 */
@SuppressWarnings("serial")
public final class SpecDeserializerException extends RuntimeException {

  SpecDeserializerException(Throwable e) {
    super(Objects.requireNonNull(e)
                 .getMessage(),
          e);
    setStackTrace(e.getStackTrace());
  }

  SpecDeserializerException(String message) {
    super(Objects.requireNonNull(message));
  }

  static SpecDeserializerException invalidSpecForRecords() {
    return new SpecDeserializerException("To deserialize `JsObj`, the spec must be either a `JsObjSpec` " +
                                         "or `oneOf(JsObjSpec... specs)`");
  }
}
