package jsonvalues.spec;

import static java.util.Objects.requireNonNull;
import static jsonvalues.spec.SpecToAvroSchema.BIGINTEGER_LOGICAL_TYPE;
import static jsonvalues.spec.SpecToAvroSchema.BIG_DECIMAL_LOGICAL_TYPE;
import static jsonvalues.spec.SpecToAvroSchema.ISO_FORMAT_LOGICAL_TYPE;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
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
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * This class provides utility methods for converting Avro data structures to JSON-values (json-values library) for
 * easier manipulation and processing.
 */
public final class AvroToJson {


  private static final String UNRESOLVABLE_UNION = "Value does not match any of the union types";
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
   * Converts a GenericRecord to a JsObj value.
   *
   * @param record The GenericRecord to convert.
   * @return The JsObj value representing the GenericRecord.
   * @throws AvroToJsonException if an error occurs during conversion.
   */
  public static JsObj convert(final GenericRecord record) {
    try {
      Schema schema = requireNonNull(record).getSchema();
      JsObj result = JsObj.empty();
      assert schema.getType() == Schema.Type.RECORD;
      for (Schema.Field field : schema.getFields()) {
        Object value = record.get(field.name());
        result = result.set(field.name(),
                            toJsValue(value,
                                      field.schema())
                           );
      }

      return result;
    } catch (SpecNotSupportedInAvroException | AvroToJsonException | MetadataNotFoundException
             | SpecToSchemaException e) {
      throw e;
    } catch (Exception e) {
      throw new AvroToJsonException(e);
    }

  }


  /**
   * Converts a GenericArray to a JsArray value.
   *
   * @param genericArray The GenericArray to convert.
   * @return The JsArray value representing the GenericArray.
   * @throws AvroToJsonException if an error occurs during conversion.
   */
  public static JsArray convert(final GenericArray<?> genericArray) {
    try {
      JsArray array = JsArray.empty();
      Schema elementType = requireNonNull(genericArray)
          .getSchema()
          .getElementType();
      for (Object item : genericArray) {
        array = array.append(toJsValue(item,
                                       elementType));
      }
      return array;
    } catch (SpecNotSupportedInAvroException | AvroToJsonException | MetadataNotFoundException
             | SpecToSchemaException e) {
      throw e;
    } catch (Exception e) {
      throw new AvroToJsonException(e);
    }
  }

  static JsObj convert(Map<?, ?> value,
                       Schema schema) {
    try {
      JsObj jsObj = JsObj.empty();
      for (Map.Entry<?, ?> entry : value.entrySet()) {
        String key = entry.getKey()
                          .toString();
        Object entryValue = entry.getValue();
        JsValue jsValue = toJsValue(entryValue,
                                    schema.getValueType());
        jsObj = jsObj.set(key,
                          jsValue);
      }
      return jsObj;
    } catch (SpecNotSupportedInAvroException | AvroToJsonException | MetadataNotFoundException
             | SpecToSchemaException e) {
      throw e;
    } catch (Exception e) {
      throw new AvroToJsonException(e);
    }
  }

  static JsValue fromUnionToJsValue(Object value,
                                    List<Schema> unionSchemas) {
    for (Schema schema : unionSchemas) {
      try {
        return toJsValue(value,
                         schema);
      } catch (Exception e) {
        AvroSpecFun.debugNonNull(e);
      }
    }
    throw new AvroToJsonException(UNRESOLVABLE_UNION);
  }

  @SuppressWarnings("ByteBufferBackingArray")
  static JsValue toJsValue(Object value,
                           Schema schema) {
    var type = schema.getType();

    //important to check at the very first if it's a union
    if (type == Schema.Type.UNION) {
      return fromUnionToJsValue(value,
                                schema.getTypes());
    }

    if (type == Schema.Type.NULL) {
      if (value == null) {
        return JsNull.NULL;
      } else {
        throw new AvroToJsonException(NULL_EXPECTED);
      }
    }

    if (type == Schema.Type.BYTES) {
      if (value instanceof ByteBuffer bf) {
        return JsBinary.of(bf.array());
      } else {
        throw new AvroToJsonException(BYTE_ARRAY_EXPECTED_FOR_BYTES);
      }
    }

    if (type == Schema.Type.BOOLEAN) {
      if (value instanceof Boolean) {
        return JsBool.of((Boolean) value);
      } else {
        throw new AvroToJsonException(BOOLEAN_EXPECTED);
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
        throw new AvroToJsonException(CHAR_SEQ_EXPECTED_FOR_STR_TYPE);
      }
    }
    if (type == Schema.Type.ENUM) {
      if (value instanceof GenericData.EnumSymbol symbol) {
        return JsStr.of(requireNonNull(symbol).toString());
      } else {
        throw new AvroToJsonException(CHAR_SEQ_EXPECTED_FOR_ENUM_TYPE);
      }
    }

    if (type == Schema.Type.INT) {
      if (value instanceof Integer) {
        return JsInt.of((int) value);
      } else {
        throw new AvroToJsonException(INT_EXPECTED);
      }
    }

    if (type == Schema.Type.LONG) {
      if (value instanceof Long) {
        return JsLong.of((long) value);
      } else {
        throw new AvroToJsonException(LONG_EXPECTED);
      }
    }

    if (type == Schema.Type.DOUBLE || type == Schema.Type.FLOAT) {
      if (value instanceof Number) {
        return JsDouble.of(((Number) value).doubleValue());
      } else {
        throw new AvroToJsonException(NUMBER_EXPECTED);
      }
    }

    if (type == Schema.Type.FIXED) {
      if (value instanceof GenericData.Fixed fixed) {
        return JsBinary.of(requireNonNull(fixed).bytes());
      } else {
        throw new AvroToJsonException(BYTE_ARRAY_EXPECTED_FOR_FIXED_TYPE);
      }

    }

    if (type == Schema.Type.RECORD) {
      if (value instanceof GenericRecord) {
        return convert((GenericRecord) value);
      } else {
        throw new AvroToJsonException(RECORD_EXPECTED);
      }
    }

    if (type == Schema.Type.ARRAY) {
      if (value instanceof GenericArray<?>) {
        return convert((GenericArray<?>) value);
      } else {
        throw new AvroToJsonException(ARRAY_EXPECTED);
      }
    }

    if (type == Schema.Type.MAP) {
      if (value instanceof Map) {
        return convert(((Map<?, ?>) value),
                       schema);
      } else {
        throw new AvroToJsonException(MAP_EXPECTED);
      }
    }
    throw new AvroToJsonException(TYPE_NOT_SUPPORTED.formatted(type.getName()));
  }


}
