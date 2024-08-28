package jsonvalues.spec;

import java.util.Objects;

/**
 * An exception class to represent the scenario where metadata is missing when transforming JSON specifications into
 * Avro Schema.
 */
@SuppressWarnings("serial")
public final class MetadataNotFoundException extends RuntimeException {

    private static final String MESSAGE = """
            In order to transform a `JsObjSpec` into an Avro Schema, \
            the specification should contain associated metadata, such \
            as a name at the very least. To accomplish this, construct \
            the `JsObjSpec` using `JsObjSpecBuilder`, allowing customization \
            of this metadata to align with your specific requirements.""";

    private static final String MESSAGE_1 = """
            In order to transform an `JsEnum` into an Avro Schema, \
            the specification should contain associated metadata, such \
            as a name at the very least. To accomplish this, construct \
            the `JsEnum` using `JsEnumBuilder`, allowing customization \
            of this metadata to align with your specific requirements.""";

    private static final String MESSAGE_2 = """
            In order to transform a `JsFixedBinary` binary into an Avro Schema, \
            the specification should contain associated metadata, such \
            as a name at the very least. To accomplish this, construct \
            the `JsFixedBinary` using `JsFixedBuilder`, allowing customization \
            of this metadata to align with your specific requirements.""";

    private MetadataNotFoundException(String message) {
        super(Objects.requireNonNull(message));
    }

    static MetadataNotFoundException errorParsingJsObSpecToSchema() {
        return new MetadataNotFoundException(MESSAGE);
    }

    static MetadataNotFoundException errorParsingEnumToSchema() {
        return new MetadataNotFoundException(MESSAGE_1);
    }

    static MetadataNotFoundException errorParsingFixedToSchema() {
        return new MetadataNotFoundException(MESSAGE_2);
    }

}
