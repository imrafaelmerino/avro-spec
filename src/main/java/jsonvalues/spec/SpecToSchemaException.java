package jsonvalues.spec;

import static java.util.Objects.requireNonNull;

/**
 * An exception class to represent the scenario when an error occurs while converting a JSON specification into an Avro
 * Schema.
 */
@SuppressWarnings("serial")
public class SpecToSchemaException extends RuntimeException {

  SpecToSchemaException(final String message) {
    super(requireNonNull(message));
  }

  SpecToSchemaException(final Exception e) {
    super(requireNonNull(e).getMessage(),
          e);
    setStackTrace(e.getStackTrace());
  }

  static SpecToSchemaException logicalTypeForStringType(String logicalType) {
    return new SpecToSchemaException("The logical type `%s` is only supported by fields of type `string`.".formatted(logicalType));
  }


}
