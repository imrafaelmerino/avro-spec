package jsonvalues.spec;

import static java.util.Objects.requireNonNull;
import static jsonvalues.spec.SpecToAvroSchema.BIGINTEGER_LOGICAL_TYPE;
import static jsonvalues.spec.SpecToAvroSchema.BIG_DECIMAL_LOGICAL_TYPE;
import static jsonvalues.spec.SpecToAvroSchema.ISO_FORMAT_LOGICAL_TYPE;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import jsonvalues.JsArray;
import jsonvalues.JsBigDec;
import jsonvalues.JsBigInt;
import jsonvalues.JsBinary;
import jsonvalues.JsBool;
import jsonvalues.JsDouble;
import jsonvalues.JsInstant;
import jsonvalues.JsInt;
import jsonvalues.JsLong;
import jsonvalues.JsNull;
import jsonvalues.JsObj;
import jsonvalues.JsStr;
import jsonvalues.JsValue;
import org.apache.avro.AvroTypeException;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.UnresolvedUnionException;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Converts an Avro object to its corresponding json-values representation based on the provided schema.
 * Find below the supported Avro types and their corresponding json-values representation:
 * <pre>
 * {@code
 * | Avro Type                              | json-values                             | Avro Class                                      |
 * |----------------------------------------|-----------------------------------------|-------------------------------------------------|
 * | null                                   | JsNull.Null                             | null                                            |
 * | boolean                                | JsBool                                  | java.lang.Boolean                               |
 * | int                                    | JsInt                                   | java.lang.Integer                               |
 * | long                                   | JsLong                                  | java.lang.Long                                  |
 * | float                                  | JsDouble                                | java.lang.Float                                 |
 * | double                                 | JsDouble                                | java.lang.Double                                |
 * | bytes                                  | JsBinary                                | java.nio.HeapByteBuffer                        |
 * | string                                 | JsStr, JsBigDec, JsBigInt, JsInstant    | java.lang.String                                |
 * | record                                 | JsObj                                   | org.apache.avro.generic.GenericData$Record    |
 * | enum                                   | JsStr                                   | org.apache.avro.generic.GenericData$EnumSymbol |
 * | array                                  | JsArray                                 | org.apache.avro.generic.GenericData$Array      |
 * | map                                    | JsObj                                   | java.util.HashMap                              |
 * | fixed                                  | JsBinary                                | org.apache.avro.generic.GenericData$Fixed      |
 *
 * And the following logical types are supported to serialize JsBigDec, JsBigInt and JsInstant, which are not supported in Avro
 *
 * | Avro Type                              | Logical Type          | json-values       | Avro Class                                      |
 * |----------------------------------------|-----------------------|-------------------|-------------------------------------------------|
 * | string                                 | bigdecimal            | JsBigDec          | java.lang.String                               |
 * | string                                 | biginteger            | JsBigInt          | java.lang.String                               |
 * | string                                 | iso-8601              | JsInstant         | java.lang.String                               |
 *
 * }
 * </pre>
 */


public final class AvroToJson {

  private static final String NULL_EXPECTED = "Schema is null, but the value is not NULL";
  private static final String BOOLEAN_EXPECTED = "Expected a Boolean value for BOOLEAN schema type";
  private static final String CHAR_SEQ_EXPECTED_FOR_STR_TYPE = "Expected a CharSequence value for STRING schema type";
  private static final String CHAR_SEQ_EXPECTED_FOR_ENUM_TYPE = "Expected a CharSequence value for ENUM schema type";
  private static final String LONG_EXPECTED = "Expected a Long value for LONG schema type";
  private static final String INT_EXPECTED = "Expected an Integer value for INT schema type";
  private static final String NUMBER_EXPECTED = "Expected a Number value for DOUBLE or FLOAT schema type";
  private static final String BYTE_ARRAY_EXPECTED_FOR_FIXED_TYPE = "Expected a byte array value for FIXED schema type";
  private static final String BYTE_ARRAY_EXPECTED_FOR_BYTES = "Expected a byte array value for BYTES schema type";
  private static final String RECORD_EXPECTED = "Expected a GenericRecord value for RECORD schema type";
  private static final String ARRAY_EXPECTED = "Expected a GenericArray value for ARRAY schema type";
  private static final String MAP_EXPECTED = "Expected a Map value for MAP schema type";
  private static final String TYPE_NOT_SUPPORTED = "Type %s not supported";

  private AvroToJson() {
  }


  /**
   * Converts an Avro GenericRecord to a JsObj value.
   *
   * @param record The GenericRecord to convert.
   * @return The JsObj value representing the GenericRecord.
   */
  public static JsObj convert(final GenericRecord record) {
    Schema schema = requireNonNull(record).getSchema();
    JsObj result = JsObj.empty();
    assert schema.getType() == Schema.Type.RECORD;
    for (Schema.Field field : schema.getFields()) {
      Object value = record.get(field.name());
      result = result.set(field.name(),
                          convert(value,
                                  field.schema())
                         );
    }
    return result;


  }


  /**
   * Converts an AVro GenericArray to a JsArray value.
   *
   * @param genericArray The GenericArray to convert.
   * @return The JsArray value representing the GenericArray.
   */
  public static JsArray convert(final GenericArray<?> genericArray) {
    JsArray array = JsArray.empty();
    Schema elementType = requireNonNull(genericArray).getSchema()
                                                     .getElementType();
    for (Object item : genericArray) {
      array = array.append(convert(item,
                                   elementType));
    }
    return array;
  }

  /**
   * Converts a given Map to its corresponding json-values representation based on the provided schema.
   *
   * @param value  the Map to be converted
   * @param schema the Avro schema defining the structure of the values in the Map
   * @return the json-values representation of the Map
   * @throws NullPointerException if either the value or schema is null
   */
  public static JsObj convert(final Map<?, ?> value,
                              final Schema schema) {
    Objects.requireNonNull(value);
    Objects.requireNonNull(schema);
    var jsObj = JsObj.empty();
    for (Map.Entry<?, ?> entry : value.entrySet()) {
      var key = entry.getKey()
                     .toString();
      var entryValue = entry.getValue();
      var jsValue = convert(entryValue,
                            schema.getValueType());
      jsObj = jsObj.set(key,
                        jsValue);
    }
    return jsObj;
  }

  static JsValue fromUnionToJsValue(Object value,
                                    Schema unionschema) {
    var unionSchemas = unionschema.getTypes();
    for (Schema schema : unionSchemas) {
      try {
        return convert(value,
                       schema);
      } catch (Exception e) {
        AvroSpecFun.debugNonNull(e);
      }
    }
    throw new UnresolvedUnionException(unionschema,
                                       value);
  }

  /**
   * Converts a given Avro value to its corresponding json-values representation based on the provided schema.
   *
   * @param value  the Avro value to be converted
   * @param schema the Avro schema defining the structure of the value
   * @return the json-values representation of the Avro value
   * @throws AvroTypeException if the value cannot be converted due to incompatible types or unsupported schema types
   */
  @SuppressWarnings("ByteBufferBackingArray")
  public static JsValue convert(Object value,
                                Schema schema) {
    var type = Objects.requireNonNull(schema)
                      .getType();

    //important to check at the very first if it's a union
    if (type == Schema.Type.UNION) {
      return fromUnionToJsValue(value,
                                schema);
    }

    if (type == Schema.Type.NULL) {
      if (value == null) {
        return JsNull.NULL;
      } else {
        throw new AvroTypeException(NULL_EXPECTED);
      }
    }

    if (type == Schema.Type.BYTES) {
      if (value instanceof ByteBuffer bf) {
        return JsBinary.of(bf.array());
      } else {
        throw new AvroTypeException(BYTE_ARRAY_EXPECTED_FOR_BYTES);
      }
    }

    if (type == Schema.Type.BOOLEAN) {
      if (value instanceof Boolean) {
        return JsBool.of((Boolean) value);
      } else {
        throw new AvroTypeException(BOOLEAN_EXPECTED);
      }
    }

    if (type == Schema.Type.STRING) {
      if (value instanceof CharSequence) {
        LogicalType logicalType = schema.getLogicalType();
        if (logicalType != null) {
          String name = logicalType.getName();
          if (name.equals(BIG_DECIMAL_LOGICAL_TYPE)) {
            return JsBigDec.of(new BigDecimal(value.toString()));
          }
          if (name.equals(BIGINTEGER_LOGICAL_TYPE)) {
            return JsBigInt.of(new BigInteger(value.toString()));
          }
          if (name.equals(ISO_FORMAT_LOGICAL_TYPE)) {
            return JsInstant.of(value.toString());
          }
        }
        return JsStr.of(value.toString());
      } else {
        throw new AvroTypeException(CHAR_SEQ_EXPECTED_FOR_STR_TYPE);
      }
    }
    if (type == Schema.Type.ENUM) {
      if (value instanceof GenericData.EnumSymbol symbol) {
        return JsStr.of(requireNonNull(symbol).toString());
      } else {
        throw new AvroTypeException(CHAR_SEQ_EXPECTED_FOR_ENUM_TYPE);
      }
    }

    if (type == Schema.Type.INT) {
      if (value instanceof Integer) {
        return JsInt.of((int) value);
      } else {
        throw new AvroTypeException(INT_EXPECTED);
      }
    }

    if (type == Schema.Type.LONG) {
      if (value instanceof Long) {
        return JsLong.of((long) value);
      } else {
        throw new AvroTypeException(LONG_EXPECTED);
      }
    }

    if (type == Schema.Type.DOUBLE || type == Schema.Type.FLOAT) {
      if (value instanceof Number) {
        return JsDouble.of(((Number) value).doubleValue());
      } else {
        throw new AvroTypeException(NUMBER_EXPECTED);
      }
    }

    if (type == Schema.Type.FIXED) {
      if (value instanceof GenericData.Fixed fixed) {
        return JsBinary.of(requireNonNull(fixed).bytes());
      } else {
        throw new AvroTypeException(BYTE_ARRAY_EXPECTED_FOR_FIXED_TYPE);
      }

    }

    if (type == Schema.Type.RECORD) {
      if (value instanceof GenericRecord) {
        return convert((GenericRecord) value);
      } else {
        throw new AvroTypeException(RECORD_EXPECTED);
      }
    }

    if (type == Schema.Type.ARRAY) {
      if (value instanceof GenericArray<?>) {
        return convert((GenericArray<?>) value);
      } else {
        throw new AvroTypeException(ARRAY_EXPECTED);
      }
    }

    if (type == Schema.Type.MAP) {
      if (value instanceof Map) {
        return convert(((Map<?, ?>) value),
                       schema);
      } else {
        throw new AvroTypeException(MAP_EXPECTED);
      }
    }
    throw new AvroTypeException(TYPE_NOT_SUPPORTED.formatted(type.getName()));
  }


}
