package jsonvalues.spec;

import jsonvalues.*;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static jsonvalues.spec.AvroConstants.*;

/**
 * This class provides functionality to convert JSON specifications (jsonvalues.spec) into Avro schemas. It includes
 * methods for converting JsObjSpec and JsArraySpec into Avro schemas.
 */
public final class SpecToAvroSchema {

    static final String BIG_DECIMAL_LOGICAL_TYPE = "bigdecimal";
    static final String BIGINTEGER_LOGICAL_TYPE = "biginteger";
    static final String ISO_FORMAT_LOGICAL_TYPE = "iso-8601";
    private static final String TYPE_DUPLICATED = "Union type with duplicate: `%s`";

    private static final String ENUM_TYPE_USELESS = "The union type `[string, enum]` can be reduced to " +
                                                    "`string` with the same meaning because `enum` is" +
                                                    " encoded as `string`";
    private static final String FIXED_TYPE_USELESS = "The union type `[bytes, fixed]` can be reduced to " +
                                                     "`bytes` with the same meaning because `fixed` is" +
                                                     " encoded as `bytes`";

    private static final String INSTANT_TYPE_USELESS = "The union type `[string, instant]` can be reduced to " +
                                                       "`string` with the same meaning because `instant` is" +
                                                       " encoded as string";

    private static final String DECIMAL_TYPE_USELESS = "The union type `[string, decimal]` can be reduced to " +
                                                       "`string` with the same meaning because `decimal` is" +
                                                       " encoded as string";

    private static final String BIGINTEGER_TYPE_USELESS = "The union type `[string, biginteger]` can be reduced to " +
                                                          "`string` with the same meaning because `biginteger` is" +
                                                          " encoded as string";
    private static final String NESTED_TYPES_NOT_ALLOWED = "Nested types are not allowed in Avro. Found the " +
                                                           "type: `%s`";
    static Schema.Parser parser = new Schema.Parser();

    static Map<String, Schema> avrocache = new HashMap<>();

    static {
        LogicalTypes.register(BIG_DECIMAL_LOGICAL_TYPE,
                              schema -> {
                                  if (schema.getType() != Schema.Type.STRING) {
                                      throw SpecToSchemaException.logicalTypeForStringType(BIG_DECIMAL_LOGICAL_TYPE);
                                  }

                                  return new LogicalType(BIG_DECIMAL_LOGICAL_TYPE);
                              });

        LogicalTypes.register(BIGINTEGER_LOGICAL_TYPE,
                              schema -> {
                                  if (schema.getType() != Schema.Type.STRING) {
                                      throw SpecToSchemaException.logicalTypeForStringType(BIGINTEGER_LOGICAL_TYPE);
                                  }

                                  return new LogicalType(BIGINTEGER_LOGICAL_TYPE);
                              });

        LogicalTypes.register(ISO_FORMAT_LOGICAL_TYPE,
                              schema -> {
                                  if (schema.getType() != Schema.Type.STRING) {
                                      throw SpecToSchemaException.logicalTypeForStringType(ISO_FORMAT_LOGICAL_TYPE);
                                  }

                                  return new LogicalType(ISO_FORMAT_LOGICAL_TYPE);
                              });


    }


    private SpecToAvroSchema() {
    }

    /**
     * Converts a {@link JsSpec} to an Avro {@link Schema}.
     * <p>
     * This method supports conversion for {@link JsObjSpec}, {@link JsArraySpec}, and generic {@link JsSpec} instances.
     * For {@link JsObjSpec}, the conversion is performed by the specialized method {@link #convert(JsObjSpec)}. For
     * {@link JsArrayOfSpec}, the conversion is performed by the specialized method {@link #convert(JsArraySpec)}.
     * Generic {@link JsSpec} instances are converted using the Avro {@link Schema.Parser} after converting them to a
     * JSON representation.
     * </p>
     *
     * @param spec The {@link JsSpec} to convert.
     * @return The Avro {@link Schema} corresponding to the given {@link JsSpec}.
     * @throws SpecToSchemaException           If an error occurs during the conversion process.
     * @throws SpecNotSupportedInAvroException If the given {@link JsSpec} is not supported in Avro schemas.
     * @throws MetadataNotFoundException       If metadata required for conversion is not found.
     */
    public static Schema convert(final JsSpec spec) {
        if (requireNonNull(spec) instanceof JsObjSpec objSpec) {
            return convert(objSpec);
        }
        if (spec instanceof JsArrayOfSpec arrSpec) {
            return convert(arrSpec);
        }
        try {
            return parser.parse(toJsSchema(spec,
                                           JsNothing.NOTHING,
                                           new ArrayList<>()
                                          )
                                        .toString()
                               );
        } catch (SpecToSchemaException | SpecNotSupportedInAvroException | MetadataNotFoundException e) {
            throw e;
        } catch (Exception e) {
            throw new SpecToSchemaException(e);
        }
    }

    /**
     * Converts a JsObjSpec into an Avro schema.
     *
     * @param spec The JSON specification for the object.
     * @return Avro schema representing the JSON specification.
     * @throws SpecNotSupportedInAvroException If the specification is not supported in Avro.
     * @throws MetadataNotFoundException       If metadata is not found in the JSON specification.
     * @throws SpecToSchemaException           If an error occurs while converting the specification into a schema.
     */
    public static Schema convert(final JsObjSpec spec) throws SpecNotSupportedInAvroException {
        if (spec.metaData == null) {
            throw MetadataNotFoundException.errorParsingJsObSpecToSchema();
        }

        var fullName = spec.metaData.getFullName();

        if (avrocache.containsKey(fullName)) {
            return avrocache.get(fullName);
        }

        try {
            //caches the schema and returns the full name in a JsStr
            var jsFullName = objSpecSchema(spec,
                                           JsNothing.NOTHING,
                                           new ArrayList<>());

            assert jsFullName.isStr(it -> it.equals(fullName)) :
                    "%s != %s".formatted(jsFullName.toString(),
                                         fullName);

            return avrocache.get(fullName);


        } catch (SpecToSchemaException | SpecNotSupportedInAvroException | MetadataNotFoundException e) {
            throw e;
        } catch (Exception e) {
            throw new SpecToSchemaException(e);
        }
    }


    /**
     * Converts a JsArraySpec into an Avro schema.
     *
     * @param spec The JSON specification for the array.
     * @return Avro schema representing the JSON specification.
     * @throws SpecNotSupportedInAvroException If the specification is not supported in Avro.
     * @throws MetadataNotFoundException       If metadata is not found in the JSON specification.
     * @throws SpecToSchemaException           If an error occurs while converting the specification into a schema.
     */
    public static Schema convert(final JsArraySpec spec) throws SpecNotSupportedInAvroException {
        try {
            return parser.parse(toJsSchema(spec,
                                           JsNothing.NOTHING,
                                           new ArrayList<>()).toString());
        } catch (SpecToSchemaException | SpecNotSupportedInAvroException | MetadataNotFoundException e) {
            throw e;
        } catch (Exception e) {
            throw new SpecToSchemaException(e);
        }


    }

    private static JsValue toJsSchema(JsSpec spec,
                                      JsValue defaultValue,
                                      List<String> parsedSchemas
                                     ) throws SpecNotSupportedInAvroException {

        if (spec instanceof Cons cons) {
            return constantSchema(cons);
        }
        if (spec instanceof JsStrSpec) {
            return strSchema(spec,
                             defaultValue);
        }
        if (spec instanceof JsStrSuchThat) {
            return strSchema(spec,
                             defaultValue);
        }

        if (spec instanceof JsIntSpec) {
            return intSchema(spec,
                             defaultValue);
        }
        if (spec instanceof JsIntSuchThat) {
            return intSchema(spec,
                             defaultValue);
        }

        if (spec instanceof JsLongSpec) {
            return longSchema(spec,
                              defaultValue);
        }
        if (spec instanceof JsLongSuchThat) {
            return longSchema(spec,
                              defaultValue);
        }

        if (spec instanceof JsDoubleSpec) {
            return doubleSchema(spec,
                                defaultValue);
        }
        if (spec instanceof JsDoubleSuchThat) {
            return doubleSchema(spec,
                                defaultValue);
        }

        if (spec instanceof JsDecimalSpec) {
            return stringSchema(BIG_DECIMAL_TYPE,
                                spec.isNullable(),
                                defaultValue);
        }

        if (spec instanceof JsDecimalSuchThat) {
            return stringSchema(BIG_DECIMAL_TYPE,
                                spec.isNullable(),
                                defaultValue);
        }

        if (spec instanceof JsBigIntSpec) {
            return stringSchema(BIG_INTEGER_TYPE,
                                spec.isNullable(),
                                defaultValue
                               );
        }
        if (spec instanceof JsBigIntSuchThat) {
            return stringSchema(BIG_INTEGER_TYPE,
                                spec.isNullable(),
                                defaultValue);
        }

        if (spec instanceof JsBooleanSpec) {
            return boolSchema(spec,
                              defaultValue);
        }
        if (spec instanceof JsBinarySpec) {
            return binarySchema(spec,
                                defaultValue);
        }
        if (spec instanceof JsBinarySuchThat) {
            return binarySchema(spec,
                                defaultValue);
        }

        if (spec instanceof JsFixedBinary fixedBinary) {
            return fixedSchema(fixedBinary,
                               defaultValue);
        }

        if (spec instanceof JsArrayOfStrSuchThat) {
            return arrayOfStringSchema(spec,
                                       defaultValue);
        }
        if (spec instanceof JsArrayOfStr) {
            return arrayOfStringSchema(spec,
                                       defaultValue);
        }
        if (spec instanceof JsArrayOfTestedStr) {
            return arrayOfStringSchema(spec,
                                       defaultValue);
        }

        if (spec instanceof JsArrayOfBigIntSuchThat) {
            return arrayOfBigIntSchema(spec,
                                       defaultValue);
        }
        if (spec instanceof JsArrayOfBigInt) {
            return arrayOfBigIntSchema(spec,
                                       defaultValue);
        }
        if (spec instanceof JsArrayOfTestedBigInt) {
            return arrayOfBigIntSchema(spec,
                                       defaultValue);
        }

        if (spec instanceof JsArrayOfIntSuchThat) {
            return arrayOfIntSchema(spec,
                                    defaultValue);
        }
        if (spec instanceof JsArrayOfInt) {
            return arrayOfIntSchema(spec,
                                    defaultValue);
        }
        if (spec instanceof JsArrayOfTestedInt) {
            return arrayOfIntSchema(spec,
                                    defaultValue);
        }

        if (spec instanceof JsArrayOfDoubleSuchThat) {
            return arrayOfDoubleSchema(spec,
                                       defaultValue);
        }
        if (spec instanceof JsArrayOfDouble) {
            return arrayOfDoubleSchema(spec,
                                       defaultValue);
        }
        if (spec instanceof JsArrayOfTestedDouble) {
            return arrayOfDoubleSchema(spec,
                                       defaultValue);
        }

        if (spec instanceof JsArrayOfLongSuchThat) {
            return arrayOfLongSchema(spec,
                                     defaultValue);
        }
        if (spec instanceof JsArrayOfLong) {
            return arrayOfLongSchema(spec,
                                     defaultValue);
        }
        if (spec instanceof JsArrayOfTestedLong) {
            return arrayOfLongSchema(spec,
                                     defaultValue);
        }

        if (spec instanceof JsArrayOfDecimal) {
            return arrayOfDecimalSchema(spec,
                                        defaultValue);
        }
        if (spec instanceof JsArrayOfTestedDecimal) {
            return arrayOfDecimalSchema(spec,
                                        defaultValue);
        }
        if (spec instanceof JsArrayOfDecimalSuchThat) {
            return arrayOfDecimalSchema(spec,
                                        defaultValue);
        }

        if (spec instanceof JsArrayOfBool) {
            return arrayOfBooleanSchema(spec,
                                        defaultValue);
        }
        if (spec instanceof JsArrayOfBoolSuchThat) {
            return arrayOfBooleanSchema(spec,
                                        defaultValue);
        }

        if (spec instanceof JsInstantSpec) {
            return instantSchema(spec,
                                 defaultValue);
        }
        if (spec instanceof JsInstantSuchThat) {
            return instantSchema(spec,
                                 defaultValue);
        }

        if (spec instanceof JsArrayOfSpec arrayOfSpec) {
            return arrayOfSpecSchema(arrayOfSpec,
                                     defaultValue,
                                     parsedSchemas);
        }
        if (spec instanceof JsObjSpec objSpec) {
            return objSpecSchema(objSpec,
                                 defaultValue,
                                 parsedSchemas);
        }

        if (spec instanceof JsMapOfInt) {
            return mapOfIntSchema(spec,
                                  defaultValue);
        }
        if (spec instanceof JsMapOfDouble) {
            return mapOfDoubleSchema(spec,
                                     defaultValue);
        }
        if (spec instanceof JsMapOfLong) {
            return mapOfLongSchema(spec,
                                   defaultValue);
        }
        if (spec instanceof JsMapOfBool) {
            return mapOfBoolSchema(spec,
                                   defaultValue);
        }
        if (spec instanceof JsMapOfBigInt) {
            return mapOfBigIntegerSchema(spec,
                                         defaultValue);
        }
        if (spec instanceof JsMapOfSpec mapOfSpec) {
            return mapOfSpecSchema(mapOfSpec,
                                   defaultValue,
                                   parsedSchemas);
        }
        if (spec instanceof JsMapOfStr) {
            return mapOfStrSchema(spec,
                                  defaultValue);
        }
        if (spec instanceof JsMapOfDec) {
            return mapOfDecSchema(spec,
                                  defaultValue);
        }
        if (spec instanceof JsMapOfBinary) {
            return mapOfBinarySchema(spec,
                                     defaultValue);
        }
        if (spec instanceof JsMapOfInstant) {
            return mapOfInstantSchema(spec,
                                      defaultValue);
        }
        if (spec instanceof JsEnum jsEnum) {
            return enumSchema(jsEnum,
                              defaultValue);
        }

        if (spec instanceof OneOf oneOf) {
            return oneOfSchema(oneOf,
                               defaultValue,
                               parsedSchemas);
        }
        if (spec instanceof NamedSpec namedSpec) {
            JsSpec cached = JsSpecCache.get(namedSpec.name);
            var alreadyParsed = parsedSchemas.contains(namedSpec.name);
            if (alreadyParsed && (cached instanceof JsObjSpec || cached instanceof JsEnum
                                  || cached instanceof JsFixedBinary)) //only object, enum and fixed can be referenced by name in avro
            {

                return namedSchema(namedSpec.name,
                                   namedSpec.isNullable(),
                                   defaultValue);

            } else {
                return toJsSchema(cached,
                                  defaultValue,
                                  parsedSchemas);
            }
        }

        throw SpecNotSupportedInAvroException.errorConvertingSpecIntoSchema(spec);

    }

    private static JsValue constantSchema(final Cons cons) {
        if ("".equals(cons.name) || cons.name == null) {
            throw SpecNotSupportedInAvroException.errorConvertingConstantIntoSchema();
        }

        return JsObj.of(TYPE_FIELD,
                        ENUM_TYPE,
                        NAME_FIELD,
                        JsStr.of(cons.name),
                        SYMBOLS_FIELD,
                        JsArray.of(cons.value)
                       );
    }


    private static JsArray oneOfSchema(OneOf js,
                                       JsValue keyDefault,
                                       List<String> parsedSchemas
                                      ) {
        var specs = js.getSpecs();
        List<JsValue> avroSchemas = new ArrayList<>();

        for (int i = 0; i < specs.size(); i++) {
            JsSpec spec = specs.get(i);
            if (spec instanceof AvroSpec) {
                avroSchemas.add(toJsSchema(spec,
                                           JsNothing.NOTHING,
                                           new ArrayList<>(parsedSchemas))); //mutable list! each branch their own list of parents
            } else {
                throw SpecNotSupportedInAvroException.errorConvertingOneOfIntoSchema(spec,
                                                                                     i);
            }
        }
        JsArray schema = JsArray.ofIterable(avroSchemas);
        validateUnionWithoutDuplicatedTypes(schema);

        if (js.isNullable()) {
            if (schema.containsValue(NULL_TYPE)) {
                return schema;
            } else {
                return keyDefault.isNull() ? schema.prepend(NULL_TYPE) : schema.append(NULL_TYPE);
            }
        } else {
            return schema;
        }
    }

    private static void validateUnionWithoutDuplicatedTypes(JsArray schema) {
        if (containsEnum(schema) && schema.containsValue(STRING_TYPE)) {
            throw new SpecToSchemaException(ENUM_TYPE_USELESS);
        }
        if (containsFixed(schema) && schema.containsValue(BINARY_TYPE)) {
            throw new SpecToSchemaException(FIXED_TYPE_USELESS);
        }
        if (containsInstant(schema) && schema.containsValue(STRING_TYPE)) {
            throw new SpecToSchemaException(INSTANT_TYPE_USELESS);
        }
        if (containsDecimal(schema) && schema.containsValue(STRING_TYPE)) {
            throw new SpecToSchemaException(DECIMAL_TYPE_USELESS);
        }
        if (containsBigInteger(schema) && schema.containsValue(STRING_TYPE)) {
            throw new SpecToSchemaException(BIGINTEGER_TYPE_USELESS);
        }

        Map<String, Integer> typeCounter = new HashMap<>();
        for (JsValue type : schema) {
            if (type instanceof JsStr name) {
                typeCounter.compute(name.value,
                                    (n, i) -> i == null ? 1 : i + 1);
            } else if (type instanceof JsObj objType) {

                var name = objType.getStr(TYPE_FIELD);
                typeCounter.compute(name,
                                    (n, i) -> i == null ? 1 : i + 1);


            } else {
                throw new SpecToSchemaException(NESTED_TYPES_NOT_ALLOWED.formatted(type));
            }
        }

        typeCounter.entrySet()
                   .stream()
                   .filter(e -> e.getValue() > 1)
                   .findFirst()
                   .ifPresent(e -> {
                       throw new SpecToSchemaException(TYPE_DUPLICATED.formatted(e.getKey()));
                   });


    }

    private static boolean containsEnum(JsArray schema) {
        for (JsValue value : schema) {
            var isEnum = value.isStr(key -> avrocache.get(key) != null
                                            && avrocache.get(key)
                                                        .getType() == Schema.Type.ENUM);
            if (isEnum) {
                return true;
            }
        }
        return false;

    }

    private static boolean containsFixed(JsArray schema) {
        for (JsValue value : schema) {
            var isEnum = value.isStr(key -> avrocache.get(key) != null
                                            && avrocache.get(key)
                                                        .getType() == Schema.Type.FIXED);
            if (isEnum) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsInstant(JsArray schema) {
        for (JsValue value : schema) {
            var isInstant = value.isObj(obj -> obj.getStr("type")
                                                  .equals("string")
                                               && ISO_FORMAT_LOGICAL_TYPE.equals(obj.getStr("logicalType")));
            if (isInstant) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsDecimal(JsArray schema) {
        for (JsValue value : schema) {
            var isInstant = value.isObj(obj -> obj.getStr("type")
                                                  .equals("string")
                                               && BIG_DECIMAL_LOGICAL_TYPE.equals(obj.getStr("logicalType")));
            if (isInstant) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsBigInteger(JsArray schema) {
        for (JsValue value : schema) {
            var isInstant = value.isObj(obj -> obj.getStr("type")
                                                  .equals("string")
                                               && BIGINTEGER_LOGICAL_TYPE.equals(obj.getStr("logicalType")));
            if (isInstant) {
                return true;
            }
        }
        return false;
    }

    private static JsValue mapOfSpecSchema(JsMapOfSpec js,
                                           JsValue keyDefault,
                                           List<String> namedSchemaParents
                                          ) {
        JsObj schema = JsObj.of(TYPE_FIELD,
                                MAP_TYPE,
                                VALUES_FIELD,
                                toJsSchema(js.getValueSpec(),
                                           JsNothing.NOTHING,
                                           namedSchemaParents));
        return getTypeSorted(js.isNullable(),
                             keyDefault,
                             schema);

    }


    private synchronized static JsValue objSpecSchema(JsObjSpec objSpec,
                                                      JsValue defaultValue,
                                                      List<String> namedSchemaParents
                                                     ) {
        var metadata = objSpec.getMetaData();
        if (metadata == null) {
            throw MetadataNotFoundException.errorParsingJsObSpecToSchema();
        }
        if (avrocache.containsKey(objSpec.metaData.getFullName())
            || namedSchemaParents.contains(metadata.getFullName())//recursive schema! already processed then return the name
        ) {
            return namedSchema(objSpec.metaData.getFullName(),
                               objSpec.isNullable(),
                               defaultValue);
        }
        namedSchemaParents.add(metadata.getFullName());

        var bindings = objSpec.getBindings();
        var schema = JsObj.of(NAME_FIELD,
                              JsStr.of(metadata.name()),
                              TYPE_FIELD,
                              AvroConstants.RECORD_TYPE);
        if (metadata.namespace() != null) {
            schema = schema.set(NAMESPACE_FIELD,
                                JsStr.of(metadata.namespace()));
        }
        if (metadata.doc() != null) {
            schema = schema.set(DOC_FIELD,
                                JsStr.of(metadata.doc()));
        }
        if (metadata.aliases() != null) {
            schema = schema.set(ALIASES_FIELD,
                                JsArray.ofStrs(metadata.aliases()));
        }
        var fields = JsArray.empty();

        var fieldsDoc = metadata.fieldsDoc();
        var fieldsAliases = metadata.fieldsAliases();
        var fieldsOrder = metadata.fieldsOrder();
        var fieldsDefault = metadata.fieldsDefault();

        for (var entry : bindings.entrySet()) {
            var spec = entry.getValue();
            var key = entry.getKey();
            var keyDefault = fieldsDefault != null && fieldsDefault.get(key) != null ?
                    fieldsDefault.get(key) :
                    JsNothing.NOTHING;

            var fieldSchema = JsObj.of(NAME_FIELD,
                                       JsStr.of(key),
                                       TYPE_FIELD,
                                       toAvro(key,
                                              keyDefault,
                                              objSpec.getRequiredFields(),
                                              spec,
                                              namedSchemaParents)
                                      );
            var doc = fieldsDoc != null ? fieldsDoc.get(key) : null;
            var order = fieldsOrder != null ? fieldsOrder.get(key) : null;
            var aliases = fieldsAliases != null ? fieldsAliases.get(key) : null;

            if (doc != null) {
                fieldSchema = fieldSchema.set(DOC_FIELD,
                                              doc);
            }
            if (order != null) {
                fieldSchema = fieldSchema.set(ORDER_FIELD,
                                              order.name());
            }
            if (keyDefault.isNotNothing()) {
                //default value of binary fields must are json strings
                fieldSchema =
                        fieldSchema.set(DEFAULT_FIELD,
                                        (keyDefault instanceof JsBinary b) ?
                                                JsStr.of(new String(b.value,
                                                                    StandardCharsets.UTF_8)) : keyDefault
                                       );
            }
            if (aliases != null) {
                fieldSchema = fieldSchema.set(ALIASES_FIELD,
                                              JsArray.ofStrs(aliases));
            }

            fields = fields.append(fieldSchema);
        }

        schema = schema.set(FIELDS_FIELD,
                            fields);

        cacheSchema(metadata.getFullName(),
                    schema);

        return getTypeSorted(objSpec.isNullable(),
                             defaultValue,
                             JsStr.of(metadata.getFullName()));
    }

    private static void cacheSchema(String fullName,
                                    JsObj schema
                                   ) {
        if (!avrocache.containsKey(fullName)) {
            avrocache.put(fullName,
                          parser.parse(schema.toString())
                         );
        }
    }


    private static JsValue toAvro(String key,
                                  JsValue keyDefault,
                                  List<String> requiredFields,
                                  JsSpec spec,
                                  List<String> namedSchemaParents
                                 ) {
        assert keyDefault != null;
        JsValue schema = toJsSchema(spec,
                                    keyDefault,
                                    namedSchemaParents);
        //if it were nullable, the above method toJsSchema would've added null to the type...
        if (!spec.isNullable() && !requiredFields.contains(key)) {
            return schema instanceof JsArray arrSchema ?
                    (keyDefault.isNull() ?
                            arrSchema.prepend(NULL_TYPE) :
                            arrSchema.append(NULL_TYPE)) :
                    (keyDefault.isNull() ?
                            JsArray.of(NULL_TYPE,
                                       schema) :
                            JsArray.of(schema,
                                       NULL_TYPE));
        }
        return schema;

    }

    private static JsValue arrayOfSpecSchema(JsArrayOfSpec arrayOfSpec,
                                             JsValue keyDefault,
                                             List<String> namedSchemaParents
                                            ) {
        JsValue items = toJsSchema(arrayOfSpec.getElemSpec(),
                                   JsNothing.NOTHING,
                                   namedSchemaParents);

        JsObj schema = JsObj.of(TYPE_FIELD,
                                ARRAY_TYPE,
                                ITEMS_FIELD,
                                items);

        return getTypeSorted(arrayOfSpec.isNullable(),
                             keyDefault,
                             schema);

    }

    private static JsValue arrayOfStringSchema(JsSpec js,
                                               JsValue keyDefault
                                              ) {
        JsObj schema = JsObj.of(TYPE_FIELD,
                                ARRAY_TYPE,
                                ITEMS_FIELD,
                                STRING_TYPE);

        return getTypeSorted(js.isNullable(),
                             keyDefault,
                             schema);
    }

    private static JsValue arrayOfBooleanSchema(JsSpec js,
                                                JsValue keyDefault
                                               ) {
        JsObj schema = JsObj.of(TYPE_FIELD,
                                ARRAY_TYPE,
                                ITEMS_FIELD,
                                BOOLEAN_TYPE);

        return getTypeSorted(js.isNullable(),
                             keyDefault,
                             schema);
    }

    private static JsValue arrayOfDecimalSchema(JsSpec js,
                                                JsValue defaultValue
                                               ) {
        return containerOfLogicalType(BIG_DECIMAL_TYPE,
                                      ARRAY_TYPE,
                                      ITEMS_FIELD,
                                      js.isNullable(),
                                      defaultValue);
    }


    private static JsValue arrayOfLongSchema(JsSpec js,
                                             JsValue keyDefault
                                            ) {
        JsObj schema = JsObj.of(TYPE_FIELD,
                                ARRAY_TYPE,
                                ITEMS_FIELD,
                                LONG_TYPE);

        return getTypeSorted(js.isNullable(),
                             keyDefault,
                             schema);

    }

    private static JsValue binarySchema(JsSpec js,
                                        JsValue keyDefault
                                       ) {
        return js.isNullable() ?
                (keyDefault.isNull() ?
                        AvroConstants.NULL_BINARY_TYPE :
                        AvroConstants.BINARY_NULL_TYPE
                ) : AvroConstants.BINARY_TYPE;

    }

    private static JsValue instantSchema(JsSpec js,
                                         JsValue keyDefault
                                        ) {
        JsObj schema = JsObj.of(TYPE_FIELD,
                                STRING_TYPE,
                                LOGICAL_TYPE_FIELD,
                                ISO_TYPE);
        return getTypeSorted(js.isNullable(),
                             keyDefault,
                             schema);
    }

    private static JsValue strSchema(JsSpec js,
                                     JsValue keyDefault
                                    ) {
        return js.isNullable() ?
                (keyDefault.isNull() ?
                        NULL_STR_TYPE :
                        STR_NULL_TYPE
                ) : STR_TYPE;
    }

    private static JsValue boolSchema(JsSpec js,
                                      JsValue keyDefault
                                     ) {
        return js.isNullable() ?
                (keyDefault.isNull() ?
                        NULL_BOOL_TYPE :
                        BOOL_NULL_TYPE
                ) : AvroConstants.BOOL_TYPE;
    }

    private static JsValue namedSchema(String name,
                                       boolean nullable,
                                       JsValue keyDefault
                                      ) {
        return nullable ?
                (keyDefault.isNull() ?
                        JsArray.of("null",
                                   name) :
                        JsStr.of(name)
                ) : JsStr.of(name);
    }

    private static JsValue stringSchema(JsStr logicalType,
                                        boolean nullable,
                                        JsValue keyDefault
                                       ) {
        JsObj schema = JsObj.of(TYPE_FIELD,
                                STRING_TYPE,
                                LOGICAL_TYPE_FIELD,
                                logicalType
                               );
        return getTypeSorted(nullable,
                             keyDefault,
                             schema);

    }

    private static JsValue getTypeSorted(boolean nullable,
                                         JsValue defaultValue,
                                         JsValue schema
                                        ) {
        return nullable ?
                (defaultValue.isNull() ?
                        JsArray.of(NULL_TYPE,
                                   schema) :
                        JsArray.of(schema,
                                   NULL_TYPE)
                ) : schema;
    }


    private static JsValue longSchema(JsSpec js,
                                      JsValue keyDefault
                                     ) {
        return js.isNullable() ?
                (keyDefault.isNull() ?
                        NULL_LONG_TYPE :
                        LONG_NULL_TYPE
                ) : LONG_TYPE;
    }

    private static JsValue intSchema(JsSpec js,
                                     JsValue keyDefault
                                    ) {
        return js.isNullable() ?
                (keyDefault.isNull() ?
                        NULL_INT_TYPE :
                        INT_NULL_TYPE
                ) : AvroConstants.INT_TYPE;
    }

    private static JsValue doubleSchema(JsSpec js,
                                        JsValue keyDefault
                                       ) {
        return js.isNullable() ?
                (keyDefault.isNull() ?
                        NULL_DOUBLE_TYPE :
                        DOUBLE_NULL_TYPE
                ) : DOUBLE_TYPE;
    }

    private synchronized static JsValue fixedSchema(JsFixedBinary js,
                                                    JsValue defaultValue
                                                   ) {
        var metadata = js.getMetaData();
        if (metadata == null) {
            throw MetadataNotFoundException.errorParsingFixedToSchema();
        }

        if (avrocache.containsKey(metadata.getFullName())) {
            return namedSchema(metadata.getFullName(),
                               js.isNullable(),
                               defaultValue);
        }

        var schema = JsObj.of(NAME_FIELD,
                              JsStr.of(metadata.name()),
                              TYPE_FIELD,
                              FIXED_TYPE,
                              SIZE_FIELD,
                              JsInt.of(js.getSize()));
        if (metadata.namespace() != null) {
            schema = schema.set(NAMESPACE_FIELD,
                                JsStr.of(metadata.namespace()));
        }

        if (metadata.doc() != null) {
            schema = schema.set(DOC_FIELD,
                                JsStr.of(metadata.doc()));
        }

        if (metadata.aliases() != null) {
            schema = schema.set(ALIASES_FIELD,
                                JsArray.ofStrs(metadata.aliases()));
        }

        cacheSchema(metadata.getFullName(),
                    schema);

        return getTypeSorted(js.isNullable(),
                             defaultValue,
                             JsStr.of(metadata.getFullName()));

    }

    private static JsValue arrayOfIntSchema(JsSpec js,
                                            JsValue keyDefault
                                           ) {
        var schema = JsObj.of(TYPE_FIELD,
                              ARRAY_TYPE,
                              ITEMS_FIELD,
                              INT_TYPE);

        return getTypeSorted(js.isNullable(),
                             keyDefault,
                             schema);
    }

    private static JsValue arrayOfDoubleSchema(JsSpec js,
                                               JsValue keyDefault
                                              ) {
        var schema = JsObj.of(TYPE_FIELD,
                              ARRAY_TYPE,
                              ITEMS_FIELD,
                              DOUBLE_TYPE);

        return getTypeSorted(js.isNullable(),
                             keyDefault,
                             schema);
    }

    private static JsValue arrayOfBigIntSchema(JsSpec js,
                                               JsValue defaultValue
                                              ) {
        return containerOfLogicalType(BIG_INTEGER_TYPE,
                                      ARRAY_TYPE,
                                      ITEMS_FIELD,
                                      js.isNullable(),
                                      defaultValue);
    }

    private static JsValue mapOfInstantSchema(JsSpec js,
                                              JsValue defaultValue
                                             ) {
        return containerOfLogicalType(ISO_TYPE,
                                      MAP_TYPE,
                                      VALUES_FIELD,
                                      js.isNullable(),
                                      defaultValue);
    }

    private static JsValue mapOfDecSchema(JsSpec js,
                                          JsValue defaultValue
                                         ) {
        return containerOfLogicalType(BIG_DECIMAL_TYPE,
                                      MAP_TYPE,
                                      VALUES_FIELD,
                                      js.isNullable(),
                                      defaultValue);
    }

    private static JsValue mapOfBinarySchema(JsSpec js,
                                             JsValue defaultValue
                                            ) {
        var schema = JsObj.of(TYPE_FIELD,
                              MAP_TYPE,
                              VALUES_FIELD,
                              BINARY_TYPE);
        return getTypeSorted(js.isNullable(),
                             defaultValue,
                             schema);
    }

    private static JsValue containerOfLogicalType(JsStr logicalType,
                                                  JsStr containerType,
                                                  String elementsKey,
                                                  boolean isNullable,
                                                  JsValue defaultValue
                                                 ) {
        var schema = JsObj.of(TYPE_FIELD,
                              STRING_TYPE,
                              LOGICAL_TYPE_FIELD,
                              logicalType);

        var mapSchema = JsObj.of(TYPE_FIELD,
                                 containerType,
                                 elementsKey,
                                 schema
                                );

        return getTypeSorted(isNullable,
                             defaultValue,
                             mapSchema);

    }

    private static JsValue mapOfStrSchema(JsSpec js,
                                          JsValue defaultValue
                                         ) {
        var schema = JsObj.of(TYPE_FIELD,
                              MAP_TYPE,
                              VALUES_FIELD,
                              STRING_TYPE);
        return getTypeSorted(js.isNullable(),
                             defaultValue,
                             schema);

    }

    private static JsValue mapOfBigIntegerSchema(JsSpec js,
                                                 JsValue defaultValue
                                                ) {
        return containerOfLogicalType(BIG_INTEGER_TYPE,
                                      MAP_TYPE,
                                      VALUES_FIELD,
                                      js.isNullable(),
                                      defaultValue);
    }

    private static JsValue mapOfBoolSchema(JsSpec js,
                                           JsValue keyDefault
                                          ) {
        var schema = JsObj.of(TYPE_FIELD,
                              MAP_TYPE,
                              VALUES_FIELD,
                              BOOLEAN_TYPE
                             );
        return getTypeSorted(js.isNullable(),
                             keyDefault,
                             schema);
    }

    private static JsValue mapOfLongSchema(JsSpec js,
                                           JsValue keyDefault
                                          ) {
        var schema = JsObj.of(TYPE_FIELD,
                              MAP_TYPE,
                              VALUES_FIELD,
                              LONG_TYPE
                             );
        return getTypeSorted(js.isNullable(),
                             keyDefault,
                             schema);

    }

    private static JsValue mapOfIntSchema(JsSpec js,
                                          JsValue keyDefault
                                         ) {
        JsObj schema = JsObj.of(TYPE_FIELD,
                                MAP_TYPE,
                                VALUES_FIELD,
                                INT_TYPE
                               );
        return getTypeSorted(js.isNullable(),
                             keyDefault,
                             schema);

    }

    private static JsValue mapOfDoubleSchema(JsSpec js,
                                             JsValue keyDefault
                                            ) {
        JsObj schema = JsObj.of(TYPE_FIELD,
                                MAP_TYPE,
                                VALUES_FIELD,
                                DOUBLE_TYPE
                               );
        return getTypeSorted(js.isNullable(),
                             keyDefault,
                             schema);

    }

    private synchronized static JsValue enumSchema(JsEnum jsEnum,
                                                   JsValue defaultValue
                                                  ) {
        var metaData = jsEnum.getMetaData();
        if (metaData == null) {
            throw MetadataNotFoundException.errorParsingEnumToSchema();
        }
        if (avrocache.containsKey(metaData.getFullName())) {
            return namedSchema(metaData.getFullName(),
                               jsEnum.isNullable(),
                               defaultValue);
        }

        var schema = JsObj.of(TYPE_FIELD,
                              ENUM_TYPE,
                              NAME_FIELD,
                              JsStr.of(metaData.name()));
        if (metaData.doc() != null) {
            schema = schema.set(DOC_FIELD,
                                JsStr.of(metaData.doc()));
        }
        if (metaData.namespace() != null) {
            schema = schema.set(NAMESPACE_FIELD,
                                JsStr.of(metaData.namespace()));
        }
        if (metaData.aliases() != null) {
            schema = schema.set(ALIASES_FIELD,
                                JsArray.ofStrs(metaData.aliases()));
        }
        if (metaData.defaultSymbol() != null) {
            schema = schema.set(DEFAULT_FIELD,
                                JsStr.of(metaData.defaultSymbol()));
        }
        schema = schema.set(SYMBOLS_FIELD,
                            jsEnum.getSymbols());

        cacheSchema(metaData.getFullName(),
                    schema);

        return getTypeSorted(jsEnum.isNullable(),
                             defaultValue,
                             JsStr.of(metaData.getFullName()));
    }


}
