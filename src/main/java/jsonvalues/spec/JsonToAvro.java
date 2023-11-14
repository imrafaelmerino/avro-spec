package jsonvalues.spec;

import jsonvalues.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.EnumSymbol;
import org.apache.avro.generic.GenericData.Fixed;
import org.apache.avro.generic.GenericRecordBuilder;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class provides utility methods for converting JSON-values (jsonvalues library) to Avro data structures for
 * compatibility with Avro data handling.
 */
public final class JsonToAvro {

    private static final String FIELD_VALUE_NOT_FOUND = "Field `%s` without default value not found in the JsObj";
    private static final String SCHEMA_INVALID = "No schema is valid";
    private static final String JS_VALUE_TYPE_NOT_SUPPORTED = "Type `%s` not supported";
    private static final String SYMBOL_NOT_FOUND = "Enum type with symbols `%s` not found in schema `%s`";
    private static final String FIXED_TYPE_NOT_FOUND = "Fixed type of size %d not found in schema `%s`";
    private static final String ARRAY_NOT_FOUND = "Array type not found in schema `%s`";


    private JsonToAvro() {
    }

    /**
     * Converts a JsArray to Avro data based on the provided JsArraySpec.
     *
     * @param arr  The JsArray to convert.
     * @param spec The JsArraySpec that defines the conversion rules.
     * @return The converted Avro data as a GenericData.Array.
     * @throws SpecNotSupportedInAvroException if the provided spec is not supported in Avro.
     * @throws JsonToAvroException             if an error occurs during conversion.
     */
    public static GenericData.Array<Object> toAvro(final JsArray arr,
                                                   final JsSpec spec
                                                  ) {

        try {
            assert spec.test(arr)
                       .isEmpty() :
                    "The JsArray doesn't conform the spec. Errors: %s".formatted(spec.test(arr));

            var schema = SpecToSchema.convert(spec);

            assert DebugUtils.debugNonNull(schema);

            return toAvro(arr, schema);
        } catch (SpecNotSupportedInAvroException | JsonToAvroException | MetadataNotFoundException
                 | SpecToSchemaException e) {
            throw e;
        } catch (Exception e) {
            throw new JsonToAvroException(e);
        }

    }

    /**
     * Converts a JsObj to Avro data based on the provided JsObjSpec.
     *
     * @param obj  The JsObj to convert.
     * @param spec The JsObjSpec that defines the conversion rules.
     * @return The converted Avro data as a GenericData.Record.
     * @throws SpecNotSupportedInAvroException if the provided spec is not supported in Avro.
     * @throws JsonToAvroException             if an error occurs during conversion.
     */
    public static GenericData.Record toAvro(final JsObj obj,
                                            final JsSpec spec
                                           ) {
        try {
            assert spec.test(obj).isEmpty() : "The JsObj doesn't conform the spec. Errors: %s".formatted(spec.test(obj));

            var schema = SpecToSchema.convert(spec);

            assert DebugUtils.debugNonNull(schema);

            var record = toRecord(obj, schema);

            assert GenericData.get().validate(schema, record) : "Avro `validate` method fails validating the record `%s` against the schema `%s`".formatted(record, schema);

            return record;
        } catch (SpecNotSupportedInAvroException | JsonToAvroException | MetadataNotFoundException
                 | SpecToSchemaException e) {
            throw e;
        } catch (Exception e) {
            throw new JsonToAvroException(e);
        }
    }


    /**
     * Converts a JsArray to Avro data based on the provided Avro schema.
     *
     * @param jsArray The JsArray to convert.
     * @param schema  The Avro schema to guide the conversion.
     * @return The converted Avro data as a GenericData.Array.
     * @throws JsonToAvroException if an error occurs during conversion.
     */
    static GenericData.Array<Object> toAvro(final JsArray jsArray,
                                            final Schema schema
                                           ) {
        try {
            assert (schema.getType() == Schema.Type.ARRAY ||
                    (schema.getType() == Schema.Type.UNION && unionContain(schema,
                                                                           Schema.Type.ARRAY)));
            var arrSchema = getArrayType(schema);
            var avroArray = new GenericData.Array<>(jsArray.size(), arrSchema);
            for (int i = 0; i < jsArray.size(); i++)
                avroArray.add(i, toAvro(jsArray.get(i), arrSchema.getElementType()
                                       ));
            assert GenericData.get().validate(schema, avroArray) : "Avro `validate` methods fails validating the Array `%s` against the schema `%s`".formatted(avroArray, schema);
            return avroArray;
        } catch (SpecNotSupportedInAvroException | JsonToAvroException | MetadataNotFoundException
                 | SpecToSchemaException e) {
            throw e;
        } catch (Exception e) {
            throw new JsonToAvroException(e);
        }
    }

    static Map<String, ?> toMap(final JsObj obj,
                                final Schema schema
                               ) {
        try {
            assert schema.getType() == Schema.Type.MAP;
            Map<String, Object> map = new HashMap<>();
            for (var key : obj.keySet()) {
                map.put(key, toAvro(obj.get(key), schema.getValueType()));
            }
            return map;
        } catch (SpecNotSupportedInAvroException | JsonToAvroException | MetadataNotFoundException
                 | SpecToSchemaException e) {
            throw e;
        } catch (Exception e) {
            throw new JsonToAvroException(e);
        }
    }

    static Object toAvro(final JsObj obj,
                         final Schema schema
                        ) {

        if (schema.getType() == Schema.Type.MAP)
            return toMap(obj, schema);
        else if (schema.getType() == Schema.Type.RECORD)
            return toRecord(obj, schema);
        else if (schema.isUnion()) {
            for (Schema type : schema.getTypes()) {
                try {
                    return toAvro(obj, type);
                } catch (Exception e) {
                    DebugUtils.debugNonNull(e);
                }
            }
            throw JsonToAvroException.unresolvableUnion(schema);
        } else throw JsonToAvroException.invalidRecordSchema(schema.getType());
    }


    static GenericData.Record toRecord(final JsObj obj,
                                       final Schema schema
                                      ) {
        try {
            assert (schema.getType() == Schema.Type.RECORD
                    || (schema.getType() == Schema.Type.UNION
                        && unionContain(schema, Schema.Type.RECORD))
            );

            List<Schema> recordSchemas = getAllType(schema,
                                                    Schema.Type.RECORD);

            if (recordSchemas.size() == 1)
                return buildRecord(obj,
                                   recordSchemas.get(0));
            //it's an union, test every schema and if none of them is valid, throw exception `SCHEMA_INVALID`
            for (Schema recordSchema : recordSchemas) {
                try {
                    return buildRecord(obj, recordSchema);
                } catch (Exception e) {
                    assert DebugUtils.debugNonNull(e);
                }

            }
            throw new JsonToAvroException(SCHEMA_INVALID);
        } catch (SpecNotSupportedInAvroException | JsonToAvroException | MetadataNotFoundException
                 | SpecToSchemaException e) {
            throw e;
        } catch (Exception e) {
            throw new JsonToAvroException(e);
        }

    }

    private static GenericData.Record buildRecord(JsObj obj, Schema recordSchema) {
        GenericRecordBuilder builder = new GenericRecordBuilder(recordSchema);
        for (var field : recordSchema.getFields()) {
            //iterar otra vez con los alias, puede que exista en un alias,
            //en ese caso leer el valor pero asociado al field
            JsValue value = obj.get(field.name());
            if (value.isNothing()) {
                value = tryWithAliases(field.aliases(), obj);
                if (value.isNotNothing())
                    builder.set(field, toAvro(value, field.schema()));
                else if (!field.hasDefaultValue())
                    throw new JsonToAvroException(FIELD_VALUE_NOT_FOUND.formatted(field.name()));
            } else builder.set(field, toAvro(value, field.schema()));
        }
        return builder.build();
    }

    private static JsValue tryWithAliases(Set<String> aliases, JsObj obj) {
        for (String alias : aliases) {
            JsValue value = obj.get(alias);
            if (value.isNotNothing()) return value;
        }
        return JsNothing.NOTHING;
    }


    static Object toAvro(JsValue value, Schema schema) {
        if (value instanceof JsStr js) return toAvroStr(schema, js);
        if (value instanceof JsInt js) return js.value;
        if (value instanceof JsLong js) return js.value;
        if (value instanceof JsBigDec js) return js.toString();
        if (value instanceof JsBigInt js) return js.toString();
        if (value instanceof JsDouble js) return js.value;
        if (value instanceof JsInstant js) return js.value.toString();
        if (value instanceof JsBool js) return js.value;
        if (value instanceof JsNull) return null;
        if (value instanceof JsBinary js) return toAvroBinary(schema, js);
        if (value instanceof JsObj js) return toAvro(js, schema);
        if (value instanceof JsArray js) return toAvro(js, schema);
        throw new JsonToAvroException(JS_VALUE_TYPE_NOT_SUPPORTED.formatted(value.getClass().getName()));
    }

    private static Comparable<? extends Comparable<?>> toAvroStr(Schema schema, JsStr js) {
        if (schema.getType() == Schema.Type.ENUM ||
            (schema.getType() == Schema.Type.UNION && unionContain(schema, Schema.Type.ENUM)))
            return new EnumSymbol(getEnumType(schema,
                                              js.value),
                                  js.value);
        else return js.value;
    }

    private static Comparable<? extends Comparable<?>> toAvroBinary(Schema schema, JsBinary js) {
        if (schema.getType() == Schema.Type.FIXED
            || (schema.getType() == Schema.Type.UNION
                && unionContain(schema, Schema.Type.FIXED))) {
            Schema fixedType = getFixedType(schema,
                                            js.value.length);

            return new Fixed(fixedType,
                             js.value);
        } else return ByteBuffer.wrap(js.value);
    }

    private static Schema getEnumType(Schema schema, String symbol) {
        return getAllType(schema, Schema.Type.ENUM)
                .stream()
                .filter(it -> it.getEnumSymbols().contains(symbol))
                .findFirst()
                .orElseThrow(() -> new JsonToAvroException(SYMBOL_NOT_FOUND.formatted(symbol,
                                                                                      schema.getFullName())));
    }

    private static Schema getFixedType(Schema schema, int size) {
        return getAllType(schema, Schema.Type.FIXED)
                .stream()
                .filter(it -> it.getFixedSize() == size)
                .findFirst()
                .orElseThrow(() -> new JsonToAvroException(FIXED_TYPE_NOT_FOUND.formatted(size, schema.getFullName())));
    }


    private static Schema getArrayType(Schema schema) {
        if (schema.getType() == Schema.Type.ARRAY) return schema;
        return schema
                .getTypes()
                .stream()
                .filter(it -> it.getType() == Schema.Type.ARRAY)
                .findFirst()
                .orElseThrow(() -> new JsonToAvroException(ARRAY_NOT_FOUND.formatted(schema.getFullName())));
    }

    private static List<Schema> getAllType(Schema schema, Schema.Type type) {
        if (schema.getType() == type) return List.of(schema);
        return schema
                .getTypes()
                .stream()
                .filter(it -> it.getType() == type)
                .toList();
    }

    private static boolean unionContain(Schema unionSchema, Schema.Type type) {
        return unionSchema.getTypes()
                          .stream()
                          .map(Schema::getType)
                          .anyMatch(it -> it == type);
    }


}
