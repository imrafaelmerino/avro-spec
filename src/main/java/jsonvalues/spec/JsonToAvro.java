package jsonvalues.spec;

import jsonvalues.*;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.UnresolvedUnionException;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericData.EnumSymbol;
import org.apache.avro.generic.GenericData.Fixed;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecordBuilder;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.*;

/**
 * Converts a json-values object to its corresponding Avro representation based on a provided Avro schema or a
 * json-values specification. Find below the supported Avro types and their corresponding json-values representation:
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

public final class JsonToAvro {

    private static final String FIELD_VALUE_NOT_FOUND = "Field `%s` without default value not found in the JsObj";
    private static final String SYMBOL_NOT_FOUND = "Enum type with symbols `%s` not found in schema `%s`";
    private static final String FIXED_TYPE_NOT_FOUND = "Fixed type of size %d not found in schema `%s`";
    private static final String ARRAY_NOT_FOUND = "Array type not found in schema `%s`";


    private JsonToAvro() {
    }


    /**
     * Converts a JsArray to an Avro array based on the provided spec.
     *
     * @param arr  The JsArray to convert.
     * @param spec The spec that defines the conversion rules.
     * @return The converted Avro data as a GenericData.Array.
     * @throws SpecNotSupportedInAvroException if the provided spec is not supported in Avro.
     */
    public static GenericData.Array<Object> convert(final JsArray arr,
                                                    final JsSpec spec
                                                   ) {
        assert spec.test(arr)
                   .isEmpty() :
                "The JsArray doesn't conform the spec. Errors: %s".formatted(spec.test(arr));

        var schema = SpecToAvroSchema.convert(spec);

        return convert(arr,
                       schema);

    }


    static Object toRecordOrMap(final JsObj obj,
                                final JsSpec spec
                               ) {
        assert spec.test(obj)
                   .isEmpty() : "The JsObj doesn't conform the spec. Errors: %s".formatted(spec.test(obj));

        var schema = SpecToAvroSchema.convert(spec);

        var record = toRecordOrMap(obj,
                                   schema);

        assert GenericData.get()
                          .validate(schema,
                                    record) :
                "Avro `validate` method fails validating the record `%s` against the schema `%s`".formatted(record,
                                                                                                            schema);

        return record;

    }

    /**
     * Converts a given JSON object to its corresponding Avro Record representation based on the provided schema.
     *
     * @param json   the json-values object to be converted
     * @param schema the Avro schema defining the structure of the data
     * @return the Avro Record representation of the json-values object
     */
    public static Record convert(final JsObj json,
                                 final Schema schema
                                ) {

        Object recordOrMap = toRecordOrMap(json,
                                           schema);
        if (recordOrMap instanceof Record record) {
            return record;
        } else {
            throw new AvroTypeException("Expecting an object spec and not a map. Use `convertValue` instead");
        }
    }

    /**
     * Converts a given JSON object to its corresponding Avro Record representation based on the provided
     * specification.
     *
     * @param json the json-values object to be converted
     * @param spec the json-values specification defining the structure of the data
     * @return the Avro Record representation of the json-values object
     */
    public static Record convert(final JsObj json,
                                 final JsSpec spec
                                ) {
        assert spec.test(json)
                   .isEmpty() :
                "The JsObj doesn't conform the spec. Errors: %s".formatted(spec.test(json));

        Object recordOrMap = toRecordOrMap(json,
                                           spec);
        if (recordOrMap instanceof Record record) {
            return record;
        } else {
            throw new AvroTypeException("Expecting an object spec and not a map. Use `convertValue` instead");
        }
    }

    /**
     * Converts a given JSON object to its corresponding Avro GenericContainer representation based on the provided
     * specification.
     *
     * @param json the json-values object to be converted
     * @param spec the json-values specification defining the structure of the data
     * @return the Avro GenericContainer representation of the json-values object
     */
    public static GenericContainer convert(final Json<?> json,
                                           final JsSpec spec
                                          ) {
        if (json instanceof JsObj obj) {
            Object converted = toRecordOrMap(obj,
                                             spec);
            if (converted instanceof Record record) {
                return record;
            } else {
                throw new AvroTypeException("Expecting an object spec and not a map. Use `convertValue` instead");
            }
        } else if (json instanceof JsArray arr) {
            return convert(arr,
                           spec);
        } else {
            throw new AvroTypeException("Expecting an object spec or an array spec");
        }

    }

    /**
     * Converts a given json object to its corresponding Avro GenericContainer representation based on the provided
     * schema.
     *
     * @param json   the json-values object to be converted
     * @param schema the Avro schema defining the structure of the data
     * @return the Avro GenericContainer representation of the json-values object
     */
    public static GenericContainer convert(final Json<?> json,
                                           final Schema schema
                                          ) {

        if (json instanceof JsObj obj) {
            Object converted = toRecordOrMap(obj,
                                             schema);
            if (converted instanceof Record record) {
                return record;
            } else {
                throw new AvroTypeException("Expecting an object spec and not a map. Use `convertValue` instead");
            }
        } else if (json instanceof JsArray arr) {
            return convert(arr,
                           schema);
        } else {
            throw new AvroTypeException("Expecting an object spec or an array spec");
        }
    }


    /**
     * Converts a JsArray to an Avro array based on the provided Avro schema.
     *
     * @param jsArray The JsArray to convert.
     * @param schema  The Avro schema to guide the conversion.
     * @return The converted Avro data as a GenericData.Array.
     */
    public static Array<Object> convert(final JsArray jsArray,
                                        final Schema schema
                                       ) {
        assert (schema.getType() == Schema.Type.ARRAY
                || (schema.getType() == Schema.Type.UNION && unionContain(schema,
                                                                          Schema.Type.ARRAY)));
        var arrSchema = getArrayType(schema);
        var avroArray = new Array<>(jsArray.size(),
                                    arrSchema);
        for (int i = 0; i < jsArray.size(); i++) {
            avroArray.add(i,
                          convertValue(jsArray.get(i),
                                       arrSchema.getElementType()
                                      ));
        }
        assert GenericData.get()
                          .validate(schema,
                                    avroArray) :
                "Avro `validate` methods fails validating the Array `%s` against the schema `%s`".formatted(avroArray,
                                                                                                            schema);
        return avroArray;

    }

    static Map<String, ?> toMap(final JsObj obj,
                                final Schema schema
                               ) {
        assert schema.getType() == Schema.Type.MAP;
        Map<String, Object> map = new HashMap<>();
        for (var key : obj.keySet()) {
            map.put(key,
                    convertValue(obj.get(key),
                                 schema.getValueType()));
        }
        assert GenericData.get()
                          .validate(schema,
                                    map) :
                "Avro `validate` method fails validating the map `%s` against the schema `%s`".formatted(map,
                                                                                                         schema);
        return map;

    }

    /**
     * Converts a JsObj to Avro based on the provided Avro schema. The returned object can be a map or a
     * GenericData.Record, depending on the schema type (in most use cases, it will be a GenericData.Record).
     *
     * @param obj    The Json object to convert.
     * @param schema The schema that defines the conversion rules.
     * @return The converted Avro data as a GenericData.Record.
     */
    static Object toRecordOrMap(final JsObj obj,
                                final Schema schema
                               ) {
        if (schema.getType() == Schema.Type.MAP) {
            return toMap(obj,
                         schema);
        } else if (schema.getType() == Schema.Type.RECORD) {
            return toRecord(obj,
                            schema);
        } else if (schema.isUnion()) {
            for (Schema type : schema.getTypes()) {
                try {
                    return toRecordOrMap(obj,
                                         type);
                } catch (Exception e) {
                    AvroSpecFun.debugNonNull(e);
                }
            }
            throw new UnresolvedUnionException(schema,
                                               obj);
        } else {
            throw new AvroTypeException("The schema `%s` can't be converted into a Record".formatted(schema.getType()));
        }
    }

    static Record toRecord(final JsObj obj,
                           final Schema schema
                          ) {
        assert (schema.getType() == Schema.Type.RECORD
                || (schema.getType() == Schema.Type.UNION
                    && unionContain(schema,
                                    Schema.Type.RECORD)));

        var recordSchemas = getAllType(schema,
                                       Schema.Type.RECORD);

        if (recordSchemas.size() == 1) {
            return buildRecord(obj,
                               recordSchemas.get(0));
        }
        //it's an union, test every schema and if none of them is valid, throw exception `SCHEMA_INVALID`
        for (Schema recordSchema : recordSchemas) {
            try {
                return buildRecord(obj,
                                   recordSchema);
            } catch (Exception e) {
                assert AvroSpecFun.debugNonNull(e);
            }

        }
        throw new UnresolvedUnionException(schema,
                                           obj);
    }

    private static Record buildRecord(JsObj obj,
                                      Schema recordSchema
                                     ) {
        GenericRecordBuilder builder = new GenericRecordBuilder(recordSchema);
        for (var field : recordSchema.getFields()) {
            //iterar otra vez con los alias, puede que exista en un alias,
            //en ese caso leer el valor pero asociado al field
            JsValue value = obj.get(field.name());
            if (value.isNothing()) {
                value = tryWithAliases(field.aliases(),
                                       obj);
                if (value.isNotNothing()) {
                    builder.set(field,
                                convertValue(value,
                                             field.schema()));
                } else if (!field.hasDefaultValue()) {
                    throw new AvroRuntimeException(FIELD_VALUE_NOT_FOUND.formatted(field.name()));
                }
            } else {
                builder.set(field,
                            convertValue(value,
                                         field.schema()));
            }
        }
        Record record = builder.build();
        assert GenericData.get()
                          .validate(recordSchema,
                                    record) :
                "Avro `validate` method fails validating the record `%s` against the schema `%s`".formatted(record,
                                                                                                            recordSchema);
        return record;
    }

    private static JsValue tryWithAliases(Set<String> aliases,
                                          JsObj obj
                                         ) {
        for (String alias : aliases) {
            JsValue value = obj.get(alias);
            if (value.isNotNothing()) {
                return value;
            }
        }
        return JsNothing.NOTHING;
    }

    public static Object convertValue(final JsValue value,
                                      final Schema schema
                                     ) {
        Objects.requireNonNull(value);
        Objects.requireNonNull(schema);

        return switch (value) {
            case JsStr js -> toAvroStr(schema,
                                       js);
            case JsInt js -> js.value;
            case JsLong js -> js.value;
            case JsBigDec js -> js.toString();
            case JsBigInt js -> js.toString();
            case JsDouble js -> js.value;
            case JsInstant js -> js.value.toString();
            case JsBool js -> js.value;
            case JsNull js -> null;
            case JsNothing js -> null;
            case JsBinary js -> toAvroBinary(schema,
                                             js);
            case JsObj js -> toRecordOrMap(js,
                                           schema);
            case JsArray js -> convert(js,
                                       schema);
        };

    }

    private static Comparable<? extends Comparable<?>> toAvroStr(Schema schema,
                                                                 JsStr js
                                                                ) {
        if (schema.getType() == Schema.Type.ENUM ||
            (schema.getType() == Schema.Type.UNION && unionContain(schema,
                                                                   Schema.Type.ENUM))) {
            return new EnumSymbol(getEnumType(schema,
                                              js.value),
                                  js.value);
        } else {
            return js.value;
        }
    }

    private static Comparable<? extends Comparable<?>> toAvroBinary(Schema schema,
                                                                    JsBinary js
                                                                   ) {
        if (schema.getType() == Schema.Type.FIXED
            || (schema.getType() == Schema.Type.UNION
                && unionContain(schema,
                                Schema.Type.FIXED))) {
            var fixedType = getFixedType(schema,
                                         js.value.length);

            return new Fixed(fixedType,
                             js.value);
        } else {
            return ByteBuffer.wrap(js.value);
        }
    }

    private static Schema getEnumType(Schema schema,
                                      String symbol
                                     ) {
        return getAllType(schema,
                          Schema.Type.ENUM
                         ).stream()
                          .filter(it -> it.getEnumSymbols()
                                          .contains(symbol))
                          .findFirst()
                          .orElseThrow(() -> new AvroRuntimeException(SYMBOL_NOT_FOUND.formatted(symbol,
                                                                                                 schema.getFullName())));
    }

    private static Schema getFixedType(Schema schema,
                                       int size
                                      ) {
        return getAllType(schema,
                          Schema.Type.FIXED)
                .stream()
                .filter(it -> it.getFixedSize() == size)
                .findFirst()
                .orElseThrow(() -> new AvroRuntimeException(FIXED_TYPE_NOT_FOUND.formatted(size,
                                                                                           schema.getFullName())));
    }

    private static Schema getArrayType(Schema schema) {
        if (schema.getType() == Schema.Type.ARRAY) {
            return schema;
        }
        return schema.getTypes()
                     .stream()
                     .filter(it -> it.getType() == Schema.Type.ARRAY)
                     .findFirst()
                     .orElseThrow(() -> new AvroRuntimeException(ARRAY_NOT_FOUND.formatted(schema.getFullName())));
    }

    private static List<Schema> getAllType(Schema schema,
                                           Schema.Type type
                                          ) {
        if (schema.getType() == type) {
            return List.of(schema);
        }
        return schema
                .getTypes()
                .stream()
                .filter(it -> it.getType() == type)
                .toList();
    }

    private static boolean unionContain(Schema unionSchema,
                                        Schema.Type type
                                       ) {
        return unionSchema.getTypes()
                          .stream()
                          .map(Schema::getType)
                          .anyMatch(it -> it == type);
    }


}
