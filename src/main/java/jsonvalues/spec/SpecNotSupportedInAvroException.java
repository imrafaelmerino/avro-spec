package jsonvalues.spec;

/**
 * An exception class to represent the scenario where a JSON specification is not supported in Avro.
 */
@SuppressWarnings("serial")
public final class SpecNotSupportedInAvroException extends RuntimeException {


    private static final String MESSAGE_1 = """
            Converting the OneOf spec into an Avro Schema is not possible \
            because the spec `%s` at index `%s` is not Avro compliance.""";

    private static final String MESSAGE_2 = """
            Converting the spec `%s` into an Avro Schema is not possible \
            because is not Avro compliance.""";

    private static final String MESSAGE_3 = """
            Converting the `const` spec into an Avro Schema is not possible \
            without providing a name for the constant. Please use the method \
            `JsSpecs.cons(name,value)`.""";

    private SpecNotSupportedInAvroException(String message) {
        super(message);
    }

    static SpecNotSupportedInAvroException errorConvertingOneOfIntoSchema(JsSpec spec,
                                                                          int index
                                                                         ) {
        return new SpecNotSupportedInAvroException(MESSAGE_1.formatted(spec.getClass()
                                                                           .getName(),
                                                                       index));
    }


    static SpecNotSupportedInAvroException errorConvertingSpecIntoSchema(JsSpec spec) {
        return new SpecNotSupportedInAvroException(MESSAGE_2.formatted(spec.getClass()
                                                                           .getName()));
    }

    static SpecNotSupportedInAvroException errorConvertingConstantIntoSchema() {
        return new SpecNotSupportedInAvroException(MESSAGE_3);
    }


}
