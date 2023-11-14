package jsonvalues.spec;

import java.util.Objects;

/**
 * Exception thrown for errors that occur during the serialization process in {@link SpecSerializer}.
 *
 */
@SuppressWarnings("serial")
public final class SpecSerializerException extends RuntimeException{

     SpecSerializerException(Throwable e) {
        super(Objects.requireNonNull(e).getMessage(), e);
        setStackTrace(e.getStackTrace());
    }

    SpecSerializerException(String message) {
        super(Objects.requireNonNull(message));
    }

    static SpecSerializerException invalidSpecForRecords(){
         return new SpecSerializerException("To serialize `JsObj`, the spec must be either a `JsObjSpec` or " +
                                            "`oneOf(JsObjSpec... specs)`");
    }
}
