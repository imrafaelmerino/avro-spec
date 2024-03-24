package jsonvalues.avro;

import static jsonvalues.spec.JsSpecs.arrayOfBigInt;
import static jsonvalues.spec.JsSpecs.arrayOfBool;
import static jsonvalues.spec.JsSpecs.arrayOfDec;
import static jsonvalues.spec.JsSpecs.arrayOfDouble;
import static jsonvalues.spec.JsSpecs.arrayOfInt;
import static jsonvalues.spec.JsSpecs.arrayOfSpec;
import static jsonvalues.spec.JsSpecs.arrayOfStr;
import static jsonvalues.spec.JsSpecs.bigInteger;
import static jsonvalues.spec.JsSpecs.binary;
import static jsonvalues.spec.JsSpecs.bool;
import static jsonvalues.spec.JsSpecs.decimal;
import static jsonvalues.spec.JsSpecs.doubleNumber;
import static jsonvalues.spec.JsSpecs.instant;
import static jsonvalues.spec.JsSpecs.integer;
import static jsonvalues.spec.JsSpecs.longInteger;
import static jsonvalues.spec.JsSpecs.mapOfBigInteger;
import static jsonvalues.spec.JsSpecs.mapOfBinary;
import static jsonvalues.spec.JsSpecs.mapOfBool;
import static jsonvalues.spec.JsSpecs.mapOfDecimal;
import static jsonvalues.spec.JsSpecs.mapOfDouble;
import static jsonvalues.spec.JsSpecs.mapOfInstant;
import static jsonvalues.spec.JsSpecs.mapOfInteger;
import static jsonvalues.spec.JsSpecs.mapOfLong;
import static jsonvalues.spec.JsSpecs.mapOfSpec;
import static jsonvalues.spec.JsSpecs.mapOfStr;
import static jsonvalues.spec.JsSpecs.ofNamedSpec;
import static jsonvalues.spec.JsSpecs.oneSpecOf;
import static jsonvalues.spec.JsSpecs.str;

import fun.gen.Combinators;
import fun.gen.Gen;
import fun.gen.StrGen;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import jio.test.pbt.PropBuilder;
import jio.test.pbt.TestFailure;
import jio.test.pbt.TestResult;
import jsonvalues.JsArray;
import jsonvalues.JsBinary;
import jsonvalues.JsBool;
import jsonvalues.JsInstant;
import jsonvalues.JsInt;
import jsonvalues.JsNull;
import jsonvalues.JsObj;
import jsonvalues.JsPath;
import jsonvalues.JsStr;
import jsonvalues.JsValue;
import jsonvalues.gen.JsArrayGen;
import jsonvalues.gen.JsBigDecGen;
import jsonvalues.gen.JsBigIntGen;
import jsonvalues.gen.JsBinaryGen;
import jsonvalues.gen.JsBoolGen;
import jsonvalues.gen.JsDoubleGen;
import jsonvalues.gen.JsInstantGen;
import jsonvalues.gen.JsIntGen;
import jsonvalues.gen.JsLongGen;
import jsonvalues.gen.JsObjGen;
import jsonvalues.gen.JsStrGen;
import jsonvalues.spec.AvroToJson;
import jsonvalues.spec.JsArraySpecParser;
import jsonvalues.spec.JsEnumBuilder;
import jsonvalues.spec.JsFixedBuilder;
import jsonvalues.spec.JsObjSpec;
import jsonvalues.spec.JsObjSpecBuilder;
import jsonvalues.spec.JsObjSpecParser;
import jsonvalues.spec.JsSpec;
import jsonvalues.spec.JsSpecs;
import jsonvalues.spec.JsonToAvro;
import jsonvalues.spec.SpecToAvroSchema;
import jsonvalues.spec.SpecToJsonSchema;
import jsonvalues.spec.SpecToSchemaException;
import jsonvalues.spec.deserializers.ObjSpecDeserializer;
import jsonvalues.spec.deserializers.ObjSpecDeserializerBuilder;
import jsonvalues.spec.serializers.SpecSerializer;
import jsonvalues.spec.serializers.SpecSerializerBuilder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


public class TestsObjDerSer {


  Supplier<String> nameGen = StrGen.alphabetic(10,
                                               20)
                                   .sample();


  private static void testSpec(JsSpec spec,
                               JsObj input
                              ) {
    testSpec(spec,
             input,
             input,
             Map.of());
  }

  private static void testSpec(JsSpec spec,
                               JsObj input,
                               Map<String, JsValue> defaults
                              ) {
    testSpec(spec,
             input,
             input,
             defaults);
  }

  private static void testSpec(JsSpec spec,
                               JsObj input,
                               JsObj expected
                              ) {
    testSpec(spec,
             input,
             expected,
             Map.of());
  }

  private static void testSpec(JsSpec spec,
                               JsObj input,
                               JsObj expected,
                               Map<String, JsValue> defaults
                              ) {

    GenericData.Record record = JsonToAvro.convert(input,
                                                   spec);

    Assertions.assertTrue(equals(expected,
                                 AvroToJson.convert(record),
                                 defaults
                                ),
                          "input -> record -> !input");

    JsObjSpecParser parser = JsObjSpecParser.of(spec);

    Assertions.assertTrue(equals(expected,
                                 parser.parse(input.toString()),
                                 defaults),
                          "input != parser.parse(input.toString())");


  }

  private static void testSpec(JsSpec spec,
                               JsArray input
                              ) {

    GenericData.Array<Object> arr = JsonToAvro.convert(input,
                                                       spec);

    Assertions.assertEquals(input,
                            AvroToJson.convert(arr),
                            "input -> avro array -> !input");

    JsArraySpecParser parser = JsArraySpecParser.of(spec);

    Assertions.assertEquals(input,
                            parser.parse(input.toString()),
                            "input != parser.parse(input.toString())");


  }

  private static boolean equals(JsObj input,
                                JsObj deserialized,
                                Map<String, JsValue> defaults) {
    System.out.println(input);
    System.out.println(deserialized);
    if (defaults.isEmpty()) {
      return input.equals(deserialized);
    }
    var xs = deserialized.filterKeys((path, value) -> path.size() != 1 || input.containsKey(path.head()
                                                                                                .asKey().name));
    if (!xs.equals(input)) {
      return false;
    }
    var ys = deserialized.filterKeys((path, value) -> path.size() != 1 || !input.containsKey(path.head()
                                                                                                 .asKey().name));
    return ys.keySet()
             .stream()
             .allMatch(key -> ys.get(key)
                                .equals(defaults.get(key)));
  }

  private static void testSerializer(JsObjSpec spec,
                                     JsObj input,
                                     JsObj expected) {

    testSerializer(spec,
                   input,
                   expected,
                   Map.of());
  }

  private static void testSerializer(JsSpec spec,
                                     JsObj input,
                                     JsObj expected,
                                     Map<String, JsValue> defaults) {

    SpecSerializer specSerializer =
        SpecSerializerBuilder.of(spec)
                             .build();
    ObjSpecDeserializer jsObjSpecDeserializer =
        ObjSpecDeserializerBuilder.of(spec)
                                  .build();

    Assertions.assertTrue(equals(expected,
                                 jsObjSpecDeserializer.deserialize(specSerializer.serialize(input)),
                                 defaults));


  }


  private static void testSerializer(JsSpec spec,
                                     JsObj input) {

    testSerializer(spec,
                   input,
                   input,
                   Map.of());
  }


  private static void testSerializer(JsSpec spec,
                                     JsObj input,
                                     Map<String, JsValue> defaults) {

    testSerializer(spec,
                   input,
                   input,
                   defaults);

  }

  @Test
  public void testBigDecimal() {
    var spec = JsObjSpecBuilder.withName("decimals")
                               .build(JsObjSpec.of("a",
                                                   decimal(),
                                                   "b",
                                                   mapOfDecimal().nullable(),
                                                   "c",
                                                   arrayOfDec()));

    JsObjGen gen = JsObjGen.of("a",
                               JsBigDecGen.arbitrary(),
                               "b",
                               Combinators.oneOf(Gen.cons(JsNull.NULL),
                                                 JsObjGen.of("1",
                                                             JsBigDecGen.arbitrary(),
                                                             "2",
                                                             JsBigDecGen.arbitrary())),
                               "c",
                               JsArrayGen.ofN(JsBigDecGen.arbitrary(),
                                              10));

    gen.sample(100)
       .forEach(obj -> {
         testSpec(spec,
                  obj);
         testSerializer(spec,
                        obj);
       });
  }


  @Test
  public void testBigInteger() {
    var spec = JsObjSpecBuilder.withName("bigintegers")
                               .build(JsObjSpec.of("a",
                                                   bigInteger(),
                                                   "b",
                                                   mapOfBigInteger().nullable(),
                                                   "c",
                                                   arrayOfBigInt()));

    JsObjGen gen = JsObjGen.of("a",
                               JsBigIntGen.arbitrary(BigInteger.ZERO,
                                                     BigInteger.TEN),
                               "b",
                               Combinators.oneOf(Gen.cons(JsNull.NULL),
                                                 JsObjGen.of("1",
                                                             JsBigIntGen.arbitrary(BigInteger.ZERO,
                                                                                   BigInteger.TEN),
                                                             "2",
                                                             JsBigIntGen.arbitrary(BigInteger.ZERO,
                                                                                   BigInteger.TEN))),
                               "c",
                               JsArrayGen.ofN(JsBigIntGen.arbitrary(BigInteger.ZERO,
                                                                    BigInteger.TEN),
                                              10));

    gen.sample(100)
       .forEach(obj -> {
         testSpec(spec,
                  obj);
         testSerializer(spec,
                        obj);
       });
  }

  @Test
  public void testInstant() {
    var spec = JsObjSpecBuilder.withName("instant")
                               .build(JsObjSpec.of("a",
                                                   instant(),
                                                   "b",
                                                   mapOfInstant()));

    JsObjGen gen = JsObjGen.of("a",
                               JsInstantGen.arbitrary(),
                               "b",
                               JsObjGen.of("1",
                                           JsInstantGen.arbitrary(),
                                           "2",
                                           JsInstantGen.arbitrary()));

    gen.sample(100)
       .forEach(obj -> {
         testSpec(spec,
                  obj);
         testSerializer(spec,
                        obj);
       });
  }


  @Test
  public void testJsObjSpec() {
    var spec = JsObjSpecBuilder.withName(nameGen.get())
                               .withFieldsDefaults(Map.of("a",
                                                          JsNull.NULL))
                               .withFieldAliases(Map.of("a",
                                                        List.of("a1")))
                               .build(JsObjSpec.of("a",
                                                   str().nullable(),
                                                   "b",
                                                   integer(),
                                                   "c",
                                                   JsEnumBuilder.withName(nameGen.get())
                                                                .build("A",
                                                                       "B"),
                                                   "d",
                                                   JsFixedBuilder.withName(nameGen.get())
                                                                 .build(1),
                                                   "e",
                                                   arrayOfStr(),
                                                   "f",
                                                   arrayOfInt(),
                                                   "g",
                                                   oneSpecOf(List.of(JsObjSpecBuilder.withName(nameGen.get())
                                                                                     .build(JsObjSpec.of("x",
                                                                                                         bool())
                                                                                                     .withAllOptKeys()),
                                                                     JsObjSpecBuilder.withName(nameGen.get())
                                                                                     .build(JsObjSpec.of("z",
                                                                                                         bool())
                                                                                                     .withAllOptKeys()))),
                                                   "h",
                                                   binary())
                                               .withAllOptKeys()

                                     );

    JsObj input1 = JsObj.of("a",
                            JsStr.of("K"),
                            "b",
                            JsInt.of(1),
                            "c",
                            JsStr.of("A"),
                            "d",
                            JsBinary.of("a".getBytes(StandardCharsets.UTF_8)),
                            "e",
                            JsArray.of("a",
                                       "b",
                                       "c"),
                            "f",
                            JsArray.of(1,
                                       2,
                                       3),
                            "g",
                            JsObj.of("z",
                                     JsBool.FALSE),
                            "h",
                            JsBinary.of("hi".getBytes(StandardCharsets.UTF_8)));
    testSpec(spec,
             input1);
    testSerializer(spec,
                   input1);

    JsObj input2 = JsObj.of("b",
                            JsInt.of(1),
                            "c",
                            JsStr.of("A"),
                            "d",
                            JsBinary.of("a".getBytes(StandardCharsets.UTF_8)),
                            "e",
                            JsArray.of("a",
                                       "b",
                                       "c"),
                            "f",
                            JsArray.of(1,
                                       2,
                                       3),
                            "g",
                            JsObj.of("z",
                                     JsBool.FALSE),
                            "h",
                            JsBinary.of("hi".getBytes(StandardCharsets.UTF_8)));

    JsObj expected2 = JsObj.of("a",
                               JsNull.NULL,
                               "b",
                               JsInt.of(1),
                               "c",
                               JsStr.of("A"),
                               "d",
                               JsBinary.of("a".getBytes(StandardCharsets.UTF_8)),
                               "e",
                               JsArray.of("a",
                                          "b",
                                          "c"),
                               "f",
                               JsArray.of(1,
                                          2,
                                          3),
                               "g",
                               JsObj.of("z",
                                        JsBool.FALSE),
                               "h",
                               JsBinary.of("hi".getBytes(StandardCharsets.UTF_8)));
    testSpec(spec,
             input2,
             expected2);

    testSerializer(spec,
                   input2,
                   expected2);

    JsObj input3 = JsObj.of("a1",
                            JsStr.of("K"),
                            "b",
                            JsInt.of(1),
                            "c",
                            JsStr.of("A"),
                            "d",
                            JsBinary.of("a".getBytes(StandardCharsets.UTF_8)),
                            "e",
                            JsArray.of("a",
                                       "b",
                                       "c"),
                            "f",
                            JsArray.of(1,
                                       2,
                                       3),
                            "g",
                            JsObj.of("z",
                                     JsBool.FALSE),
                            "h",
                            JsBinary.of("hi".getBytes(StandardCharsets.UTF_8)));

    JsObj expected3 = input1;
    testSpec(spec,
             input3,
             expected3);
    testSerializer(spec,
                   input3,
                   expected3);

  }

  @Test
  public void testValidateNotDuplicatedArrays() {

    var spec = JsObjSpecBuilder.withName(nameGen.get())
                               .build(JsObjSpec.of("a",
                                                   oneSpecOf(arrayOfDouble(),
                                                             arrayOfInt())));

    Assertions.assertThrows(SpecToSchemaException.class,
                            () -> SpecToAvroSchema.convert(spec));

  }

  @Test
  public void testValidateNotDuplicatedMaps() {

    var spec = JsObjSpecBuilder.withName(nameGen.get())
                               .build(JsObjSpec.of("a",
                                                   oneSpecOf(mapOfDouble(),
                                                             mapOfBool())));

    Assertions.assertThrows(SpecToSchemaException.class,
                            () -> SpecToAvroSchema.convert(spec));

  }


  @Test
  public void testValidateNotDuplicatedStr() {

    var spec = JsObjSpecBuilder.withName(nameGen.get())
                               .build(JsObjSpec.of("a",
                                                   oneSpecOf(str(),
                                                             str(s -> !s.isBlank())))
                                     );

    Assertions.assertThrows(SpecToSchemaException.class,
                            () -> SpecToAvroSchema.convert(spec));

  }


  @Test
  public void testValidateNotDuplicatedRecord() {

    var spec = JsObjSpecBuilder.withName(nameGen.get())
                               .withNamespace("org")
                               .build(JsObjSpec.of("a",
                                                   integer()));

    var invalid = JsObjSpecBuilder.withName(nameGen.get())
                                  .build(JsObjSpec.of("a",
                                                      oneSpecOf(spec,
                                                                spec)));

    Assertions.assertThrows(SpecToSchemaException.class,
                            () -> SpecToAvroSchema.convert(invalid)
                           );

  }

  @Test
  public void oneOfEnum() {

    JsSpec enum1 = JsEnumBuilder.withName(nameGen.get())
                                .build("A",
                                       "B",
                                       "C");
    JsSpec enum2 = JsEnumBuilder.withName(nameGen.get())
                                .build("a",
                                       "b",
                                       "c");
    JsSpec oneEnum = oneSpecOf(List.of(enum1,
                                       enum2));

    JsObjSpec spec = JsObjSpecBuilder.withName(nameGen.get())
                                     .build(JsObjSpec.of("key",
                                                         oneEnum));

    testSpec(spec,
             JsObj.of("key",
                      JsStr.of("a")));
  }

  @Test
  public void testOptionalFields() {

    JsObjSpec spec = JsObjSpecBuilder.withName(nameGen.get())
                                     .withFieldsDefaults(Map.of("a",
                                                                JsInt.of(1),
                                                                "b",
                                                                JsStr.of("a")))
                                     .build(JsObjSpec.of("a",
                                                         integer().nullable(),
                                                         "b",
                                                         str().nullable())
                                                     .withAllOptKeys()

                                           );

    testSpec(spec,
             JsObj.empty(),
             JsObj.of("a",
                      JsInt.of(1),
                      "b",
                      JsStr.of("a")));


  }

  @Test
  public void testAliasesFields() {

    JsObjSpec spec = JsObjSpecBuilder.withName(nameGen.get())
                                     .withFieldAliases(Map.of("a",
                                                              List.of("a1",
                                                                      "a2"),
                                                              "b",
                                                              List.of("b1",
                                                                      "b2"))
                                                      )
                                     .build(JsObjSpec.of("a",
                                                         integer().nullable(),
                                                         "b",
                                                         str().nullable())
                                                     .withAllOptKeys());

    testSpec(spec,
             JsObj.of("a1",
                      JsInt.of(1),
                      "b1",
                      JsStr.of("a")),
             JsObj.of("a",
                      JsInt.of(1),
                      "b",
                      JsStr.of("a"))
            );

    testSpec(spec,
             JsObj.of("a2",
                      JsInt.of(1),
                      "b2",
                      JsStr.of("a")
                     ),
             JsObj.of("a",
                      JsInt.of(1),
                      "b",
                      JsStr.of("a")));


  }

  @Test
  public void testPrimitives() {

    JsObjSpec spec = JsObjSpec.of("str",
                                  str().nullable(),
                                  "int",
                                  integer().nullable(),
                                  "long",
                                  longInteger().nullable(),
                                  "bigint",
                                  bigInteger().nullable(),
                                  "decimal",
                                  decimal().nullable(),
                                  "double",
                                  doubleNumber().nullable(),
                                  "boolean",
                                  bool().nullable(),
                                  "binary",
                                  binary().nullable(),
                                  "instant",
                                  instant().nullable(),
                                  "fixed",
                                  JsFixedBuilder.withName(nameGen.get())
                                                .build(2)
                                                .nullable(),
                                  "enum",
                                  JsEnumBuilder.withName(nameGen.get())
                                               .build("a",
                                                      "b")
                                               .nullable())
                              .withAllOptKeys();

    Map<String, JsValue> defaults = new HashMap<>();
    defaults.put("str",
                 JsNull.NULL);
    defaults.put("int",
                 JsNull.NULL);
    defaults.put("long",
                 JsNull.NULL);
    defaults.put("bigint",
                 JsNull.NULL);
    defaults.put("decimal",
                 JsNull.NULL);
    defaults.put("double",
                 JsNull.NULL);
    defaults.put("boolean",
                 JsNull.NULL);
    defaults.put("binary",
                 JsNull.NULL);
    defaults.put("instant",
                 JsNull.NULL);
    defaults.put("fixed",
                 JsNull.NULL);
    defaults.put("enum",
                 JsNull.NULL);

    var recordSpec = JsObjSpecBuilder.withName("specname")
                                     .withFieldsDefaults(defaults)
                                     .build(spec);

    JsObjGen gen = JsObjGen.of("str",
                               JsStrGen.alphabetic(),
                               "int",
                               JsIntGen.arbitrary(),
                               "long",
                               JsLongGen.arbitrary(),
                               "bigint",
                               JsBigIntGen.arbitrary(BigInteger.ZERO,
                                                     BigInteger.TEN),
                               "decimal",
                               JsBigDecGen.arbitrary(),
                               "double",
                               JsDoubleGen.arbitrary(),
                               "boolean",
                               JsBoolGen.arbitrary(),
                               "binary",
                               JsBinaryGen.arbitrary(1,
                                                     10),
                               "instant",
                               JsInstantGen.arbitrary(),
                               "fixed",
                               JsBinaryGen.arbitrary(2,
                                                     2),
                               "enum",
                               Combinators.oneOf(JsStr.of("a"),
                                                 JsStr.of("b")))
                           .withAllOptKeys()
                           .withAllNullValues();

    gen.sample(100)
       .forEach(obj -> {
         testSpec(recordSpec,
                  obj,
                  defaults);
         //testSerializer(recordSpec, obj, defaults);
       });

  }

  @Test
  public void testArrays() {

    JsObjSpec spec = JsObjSpec.of("str",
                                  arrayOfStr().nullable(),
                                  "int",
                                  arrayOfInt().nullable(),
                                  "long",
                                  JsSpecs.arrayOfLong()
                                         .nullable(),
                                  "bigint",
                                  arrayOfBigInt().nullable(),
                                  "decimal",
                                  arrayOfDec().nullable(),
                                  "double",
                                  arrayOfDouble().nullable(),
                                  "boolean",
                                  arrayOfBool().nullable(),
                                  "binary",
                                  arrayOfSpec(binary()).nullable(),
                                  "instant",
                                  arrayOfSpec(instant()).nullable(),
                                  "fixed",
                                  arrayOfSpec(JsFixedBuilder.withName(nameGen.get())
                                                            .build(2)).nullable(),
                                  "enum",
                                  arrayOfSpec(JsEnumBuilder.withName(nameGen.get())
                                                           .build("a",
                                                                  "b"))
                                      .nullable())
                              .withAllOptKeys();

    Map<String, JsValue> defaults = new HashMap<>();
    defaults.put("str",
                 JsNull.NULL);
    defaults.put("int",
                 JsNull.NULL);
    defaults.put("long",
                 JsNull.NULL);
    defaults.put("bigint",
                 JsNull.NULL);
    defaults.put("decimal",
                 JsNull.NULL);
    defaults.put("double",
                 JsNull.NULL);
    defaults.put("boolean",
                 JsNull.NULL);
    defaults.put("binary",
                 JsNull.NULL);
    defaults.put("instant",
                 JsNull.NULL);
    defaults.put("fixed",
                 JsNull.NULL);
    defaults.put("enum",
                 JsNull.NULL);

    var recordSpec = JsObjSpecBuilder.withName(nameGen.get())
                                     .withFieldsDefaults(defaults)
                                     .build(spec);

    JsObjGen gen = JsObjGen.of("str",
                               JsArrayGen.ofN(JsStrGen.alphabetic(),
                                              10),
                               "int",
                               JsArrayGen.ofN(JsIntGen.arbitrary(),
                                              10),
                               "long",
                               JsArrayGen.ofN(JsLongGen.arbitrary(),
                                              10),
                               "bigint",
                               JsArrayGen.ofN(JsBigIntGen.arbitrary(BigInteger.ZERO,
                                                                    BigInteger.TEN),
                                              10),
                               "decimal",
                               JsArrayGen.ofN(JsBigDecGen.arbitrary(),
                                              10),
                               "double",
                               JsArrayGen.ofN(JsDoubleGen.arbitrary(),
                                              10),
                               "boolean",
                               JsArrayGen.ofN(JsBoolGen.arbitrary(),
                                              10),
                               "binary",
                               JsArrayGen.ofN(JsBinaryGen.arbitrary(1,
                                                                    10),
                                              10),
                               "instant",
                               JsArrayGen.ofN(JsInstantGen.arbitrary(),
                                              10),
                               "fixed",
                               JsArrayGen.ofN(JsBinaryGen.arbitrary(2,
                                                                    2),
                                              10),
                               "enum",
                               JsArrayGen.ofN(Combinators.oneOf(JsStr.of("a"),
                                                                JsStr.of("b")),
                                              10)
                              )
                           .withAllOptKeys()
                           .withAllNullValues();

    gen.sample(100)
       .forEach(obj -> {
         testSpec(recordSpec,
                  obj,
                  defaults);
         testSerializer(recordSpec,
                        obj,
                        defaults);
       });

  }

  @Test
  public void testMaps() {

    JsObjSpec spec = JsObjSpec.of("str",
                                  mapOfStr().nullable(),
                                  "int",
                                  mapOfInteger().nullable(),
                                  "long",
                                  mapOfLong().nullable(),
                                  "bigint",
                                  mapOfBigInteger().nullable(),
                                  "decimal",
                                  mapOfDecimal().nullable(),
                                  "double",
                                  mapOfDouble().nullable(),
                                  "boolean",
                                  mapOfBool().nullable(),
                                  "binary",
                                  mapOfBinary().nullable(),
                                  "instant",
                                  mapOfInstant().nullable(),
                                  "fixed",
                                  mapOfSpec(JsFixedBuilder.withName(nameGen.get())
                                                          .build(2)).nullable(),
                                  "enum",
                                  mapOfSpec(JsEnumBuilder.withName(nameGen.get())
                                                         .build("a",
                                                                "b"))
                                      .nullable()
                                 )
                              .withAllOptKeys();

    Map<String, JsValue> defaults = new HashMap<>();
    defaults.put("str",
                 JsNull.NULL);
    defaults.put("int",
                 JsNull.NULL);
    defaults.put("long",
                 JsNull.NULL);
    defaults.put("bigint",
                 JsNull.NULL);
    defaults.put("decimal",
                 JsNull.NULL);
    defaults.put("double",
                 JsNull.NULL);
    defaults.put("boolean",
                 JsNull.NULL);
    defaults.put("binary",
                 JsNull.NULL);
    defaults.put("instant",
                 JsNull.NULL);
    defaults.put("fixed",
                 JsNull.NULL);
    defaults.put("enum",
                 JsNull.NULL);

    var recordSpec = JsObjSpecBuilder.withName(nameGen.get())
                                     .withFieldsDefaults(defaults)
                                     .build(spec);

    JsObjGen gen = JsObjGen.of("str",
                               JsObjGen.of("a",
                                           JsStrGen.alphabetic(),
                                           "b",
                                           JsStrGen.alphabetic()),
                               "int",
                               JsObjGen.of("a",
                                           JsIntGen.arbitrary(),
                                           "b",
                                           JsIntGen.arbitrary()),
                               "long",
                               JsObjGen.of("a",
                                           JsLongGen.arbitrary(),
                                           "b",
                                           JsLongGen.arbitrary()),
                               "bigint",
                               JsObjGen.of("a",
                                           JsBigIntGen.arbitrary(BigInteger.ZERO,
                                                                 BigInteger.TEN),
                                           "b",
                                           JsBigIntGen.arbitrary(BigInteger.ZERO,
                                                                 BigInteger.TEN)),
                               "decimal",
                               JsObjGen.of("a",
                                           JsBigDecGen.arbitrary(),
                                           "b",
                                           JsBigDecGen.arbitrary()),
                               "double",
                               JsObjGen.of("a",
                                           JsDoubleGen.arbitrary(),
                                           "b",
                                           JsDoubleGen.arbitrary()),
                               "boolean",
                               JsObjGen.of("a",
                                           JsBoolGen.arbitrary(),
                                           "b",
                                           JsBoolGen.arbitrary()),
                               "binary",
                               JsObjGen.of("a",
                                           JsBinaryGen.arbitrary(1,
                                                                 10),
                                           "b",
                                           JsBinaryGen.arbitrary(1,
                                                                 10)),
                               "instant",
                               JsObjGen.of("a",
                                           JsInstantGen.arbitrary(),
                                           "b",
                                           JsInstantGen.arbitrary()),
                               "fixed",
                               JsObjGen.of("a",
                                           JsBinaryGen.arbitrary(2,
                                                                 2),
                                           "b",
                                           JsBinaryGen.arbitrary(2,
                                                                 2)),
                               "enum",
                               JsObjGen.of("a",
                                           Combinators.oneOf(JsStr.of("a"),
                                                             JsStr.of("b")),
                                           "b",
                                           Combinators.oneOf(JsStr.of("a"),
                                                             JsStr.of("b"))))
                           .withAllOptKeys()
                           .withAllNullValues();

    gen.sample(100)
       .forEach(obj -> {
         testSpec(recordSpec,
                  obj,
                  defaults);
         testSerializer(recordSpec,
                        obj,
                        defaults);
       });

  }

  @Test
  public void testUnions() {

    var recordSpec = JsObjSpecBuilder.withName(nameGen.get())
                                     .withNamespace(nameGen.get())
                                     .build(JsObjSpec.of("a",
                                                         oneSpecOf(str(),
                                                                   bool()).nullable())

                                           );

    var gen = JsObjGen.of("a",
                          Combinators.oneOf(JsStrGen.alphabetic(),
                                            JsBoolGen.arbitrary(),
                                            Gen.cons(JsNull.NULL)));

    gen.sample(100)
       .forEach(obj -> {
         testSpec(recordSpec,
                  obj,
                  obj);
         testSerializer(recordSpec,
                        obj,
                        obj);
       });


  }

  @Test
  public void test_Union_String_Enum_Error_Because_doesnt_make_sense() {

    var spec = JsObjSpec.of("a",
                            oneSpecOf(str(),
                                      JsEnumBuilder.withName(nameGen.get())
                                                   .build("a",
                                                          "b")
                                     )
                           );
    var recordSpec = JsObjSpecBuilder.withName(nameGen.get())
                                     .build(spec);

    Assertions.assertThrows(SpecToSchemaException.class,
                            () -> SpecToAvroSchema.convert(recordSpec));


  }

  @Test
  public void test_Fixed_String_Enum_Error_Because_doesnt_make_sense() {

    var spec = JsObjSpec.of("a",
                            oneSpecOf(binary(),
                                      JsFixedBuilder.withName(nameGen.get())
                                                    .build(2)
                                     )
                           );
    var recordSpec = JsObjSpecBuilder.withName(nameGen.get())
                                     .build(spec);

    Assertions.assertThrows(SpecToSchemaException.class,
                            () -> SpecToAvroSchema.convert(recordSpec));


  }

  @Test
  public void test_Union_Duplicated_Name_Space() {

    var spec = JsObjSpecBuilder.withName(nameGen.get())
                               .build(JsObjSpec.of("a",
                                                   str()));

    var spec1 = JsObjSpecBuilder.withName(nameGen.get())
                                .build(JsObjSpec.of("a",
                                                    oneSpecOf(spec,
                                                              spec)));

    Assertions.assertThrows(SpecToSchemaException.class,
                            () -> SpecToAvroSchema.convert(spec1));


  }


  @Test
  public void test_Union_Root_Type() {

    var spec1 = JsObjSpecBuilder.withName(nameGen.get())
                                .build(JsObjSpec.of("a",
                                                    oneSpecOf(bool(),
                                                              str())));

    var spec2 = JsObjSpecBuilder.withName(nameGen.get())
                                .build(JsObjSpec.of("b",
                                                    oneSpecOf(instant(),
                                                              bool())));

    var union = oneSpecOf(spec1,
                          spec2);

    testSpec(union,
             JsObj.of("a",
                      JsBool.FALSE));
    testSerializer(union,
                   JsObj.of("a",
                            JsBool.FALSE));
    testSpec(union,
             JsObj.of("a",
                      JsStr.of("hi!")));
    testSerializer(union,
                   JsObj.of("a",
                            JsStr.of("hi!")));
    testSpec(union,
             JsObj.of("b",
                      JsBool.TRUE));
    testSerializer(union,
                   JsObj.of("b",
                            JsBool.TRUE));
    testSpec(union,
             JsObj.of("b",
                      JsInstant.of(Instant.now())));
    testSerializer(union,
                   JsObj.of("b",
                            JsInstant.of(Instant.now())));


  }


  @Test
  public void testRecursiveType() {

    var spec = JsObjSpecBuilder.withName("person")
                               .withFieldsDefaults(Map.of("father",
                                                          JsNull.NULL))
                               .build(JsObjSpec.of("name",
                                                   str(),
                                                   "age",
                                                   integer(),
                                                   "father",
                                                   ofNamedSpec("person").nullable()
                                                  )
                                               .withOptKeys("father")
                                     );

    var person = JsObj.of("name",
                          JsStr.of("Rafa"),
                          "age",
                          JsInt.of(40),
                          "father",
                          JsObj.of("name",
                                   JsStr.of("Luis"),
                                   "age",
                                   JsInt.of(73)
                                  )
                         );

    testSerializer(spec,
                   person,
                   person.set(JsPath.path("/father/father"),
                              JsNull.NULL));


  }

  @Test
  public void testBinaryDefaults() {

    JsObjSpec spec = JsObjSpecBuilder.withName(nameGen.get())
                                     .withNamespace("a.b.c")
                                     .withDoc("doc")
                                     .withAliases(List.of(nameGen.get(),
                                                          nameGen.get())
                                                 )
                                     .withFieldsDefaults(Map.of("a",
                                                                JsBinary.of(new byte[]{110, 111}),
                                                                "b",
                                                                JsBool.FALSE,
                                                                "c",
                                                                JsStr.of("enum1"),
                                                                "d",
                                                                JsBinary.of(new byte[]{110, 111}))
                                                        )
                                     .withFieldAliases(Map.of("a",
                                                              List.of("b",
                                                                      "c")))
                                     .withFieldDocs(Map.of("a",
                                                           "a doc",
                                                           "b",
                                                           "b doc"))
                                     .withFieldOrders(Map.of("a",
                                                             JsObjSpecBuilder.ORDERS.ascending,
                                                             "b",
                                                             JsObjSpecBuilder.ORDERS.descending)
                                                     )
                                     .build(JsObjSpec.of("a",
                                                         binary(),
                                                         "b",
                                                         bool(),
                                                         "c",
                                                         JsEnumBuilder.withName(nameGen.get())
                                                                      .withNamespace("a.b.c")
                                                                      .withAliases(List.of("alias1",
                                                                                           "alias2"))
                                                                      .withDoc("doc enum")
                                                                      .withDefaultSymbol("enum1")
                                                                      .build("enum1",
                                                                             "enum2"),
                                                         "d",
                                                         JsFixedBuilder.withName(nameGen.get())
                                                                       .withDoc("fixed doc")
                                                                       .withNamespace("a.b.c")
                                                                       .withAliases(List.of("alias3",
                                                                                            "alias4"))
                                                                       .build(2)
                                                        )
                                                     .withAllOptKeys()
                                           );

    Schema schema = SpecToAvroSchema.convert(spec);

    Assertions.assertNotNull(schema);

    System.out.println(schema);

    var record = JsonToAvro.convert(JsObj.empty(),
                                    spec
                                   );

    var obj = AvroToJson.convert(record);

    Assertions.assertEquals(JsObj.of("a",
                                     JsBinary.of(new byte[]{110, 111}),
                                     "b",
                                     JsBool.FALSE,
                                     "c",
                                     JsStr.of("enum1"),
                                     "d",
                                     JsBinary.of(new byte[]{110, 111})),
                            obj);

  }

  @Test
  public void testRootArrays() {

    JsSpec spec = arrayOfSpec(oneSpecOf(str(),
                                        integer()
                                       )
                                  .nullable()
                             );

    JsSpec spec1 = oneSpecOf(arrayOfSpec(oneSpecOf(bool(),
                                                   longInteger()
                                                  )
                                             .nullable()
                                        )
                            );

    testSpec(spec,
             JsArray.of("a"));
    testSpec(spec,
             JsArray.of(1,
                        2));

    testSpec(spec1,
             JsArray.of(true,
                        false));
    testSpec(spec1,
             JsArray.of(1L,
                        2L));


  }

  @Test
  public void testConcurrency() {
    JsObjSpec spec = JsObjSpec.of("str",
                                  str().nullable(),
                                  "int",
                                  integer().nullable(),
                                  "long",
                                  longInteger().nullable(),
                                  "bigint",
                                  bigInteger().nullable(),
                                  "decimal",
                                  decimal().nullable(),
                                  "double",
                                  doubleNumber().nullable(),
                                  "boolean",
                                  bool().nullable(),
                                  "binary",
                                  binary().nullable(),
                                  "instant",
                                  instant().nullable(),
                                  "fixed",
                                  JsFixedBuilder.withName(nameGen.get())
                                                .build(2)
                                                .nullable(),
                                  "enum",
                                  JsEnumBuilder.withName(nameGen.get())
                                               .build("a",
                                                      "b")
                                               .nullable())
                              .withAllOptKeys();

    Map<String, JsValue> defaults = new HashMap<>();
    defaults.put("str",
                 JsNull.NULL);
    defaults.put("int",
                 JsNull.NULL);
    defaults.put("long",
                 JsNull.NULL);
    defaults.put("bigint",
                 JsNull.NULL);
    defaults.put("decimal",
                 JsNull.NULL);
    defaults.put("double",
                 JsNull.NULL);
    defaults.put("boolean",
                 JsNull.NULL);
    defaults.put("binary",
                 JsNull.NULL);
    defaults.put("instant",
                 JsNull.NULL);
    defaults.put("fixed",
                 JsNull.NULL);
    defaults.put("enum",
                 JsNull.NULL);

    var recordSpec = JsObjSpecBuilder.withName(nameGen.get())
                                     .withFieldsDefaults(defaults)
                                     .build(spec);

    JsObjGen gen = JsObjGen.of("str",
                               JsStrGen.alphabetic(),
                               "int",
                               JsIntGen.arbitrary(),
                               "long",
                               JsLongGen.arbitrary(),
                               "bigint",
                               JsBigIntGen.arbitrary(BigInteger.ZERO,
                                                     BigInteger.TEN),
                               "decimal",
                               JsBigDecGen.arbitrary(),
                               "double",
                               JsDoubleGen.arbitrary(),
                               "boolean",
                               JsBoolGen.arbitrary(),
                               "binary",
                               JsBinaryGen.arbitrary(1,
                                                     10),
                               "instant",
                               JsInstantGen.arbitrary(),
                               "fixed",
                               JsBinaryGen.arbitrary(2,
                                                     2),
                               "enum",
                               Combinators.oneOf(JsStr.of("a"),
                                                 JsStr.of("b")))
                           .withAllOptKeys()
                           .withAllNullValues();

    SpecSerializer serializer = SpecSerializerBuilder.of(recordSpec)
                                                     .build();
    ObjSpecDeserializer deserializer = ObjSpecDeserializerBuilder.of(recordSpec)
                                                                 .build();

    Function<JsObj, TestResult> fun = obj -> {
      JsObj xs = deserializer.deserialize(serializer.serialize(obj));
      return equals(obj,
                    xs,
                    defaults)
             ? TestResult.SUCCESS :
             TestFailure.reason("Different than generated: %s".formatted(xs));
    };
    PropBuilder.of("input -> bytes[] -> input",
                   gen,
                   fun)
               .build()
               .repeatPar(100)
               .check()
               .assertAllSuccess();

  }

  public static void main(String[] args)  {
    String NAME_FIELD = "name";
    String TYPE_FIELD = "type";
    String BUTTON_COUNT_FIELD = "buttonCount";
    String WHEEL_COUNT_FIELD = "wheelCount";
    String TRACKING_TYPE_FIELD = "trackingType";
    String KEY_COUNT_FIELD = "keyCount";
    String MEDIA_BUTTONS_FIELD = "mediaButtons";
    String CONNECTED_DEVICES_FIELD = "connectedDevices";
    String PERIPHERAL_FIELD = "peripheral";
    JsSpec TRACKING_TYPE_ENUM = JsEnumBuilder.withName("tracking")
                                             .build(
                                                 "ball",
                                                 "optical");
    var baseSpec =
        JsObjSpec.of(NAME_FIELD,
                     JsSpecs.str()
                    );

    var mouseSpec =
        JsObjSpec.of(TYPE_FIELD,
                     JsSpecs.cons("mouse_type",
                                  JsStr.of("mouse")),
                     BUTTON_COUNT_FIELD,
                     JsSpecs.integer(),
                     WHEEL_COUNT_FIELD,
                     JsSpecs.integer(),
                     TRACKING_TYPE_FIELD,
                     TRACKING_TYPE_ENUM
                    )
                 .concat(baseSpec);

    var keyboardSpec =
        JsObjSpec.of(TYPE_FIELD,
                     JsSpecs.cons("keyboard_type",
                                  JsStr.of("keyboard")),
                     KEY_COUNT_FIELD,
                     JsSpecs.integer(),
                     MEDIA_BUTTONS_FIELD,
                     JsSpecs.bool()
                    )
                 .concat(baseSpec);

    var usbHubSpec =
        JsObjSpecBuilder.withName("usb_hub")
                        .withFieldsDefaults(Map.of(CONNECTED_DEVICES_FIELD,
                                                   JsNull.NULL))
                        .build(JsObjSpec.of(
                                            TYPE_FIELD,
                                            JsSpecs.cons("usbHub_type",
                                                         JsStr.of("usbHub")),
                                            CONNECTED_DEVICES_FIELD,
                                            JsSpecs.arrayOfSpec(JsSpecs.ofNamedSpec(PERIPHERAL_FIELD))
                                                   .nullable()
                                           )
                                        .withOptKeys(CONNECTED_DEVICES_FIELD)
                                        .concat(baseSpec));

    var peripheralSpec =
        JsSpecs.ofNamedSpec(PERIPHERAL_FIELD,
                            oneSpecOf(JsSpecs.ofNamedSpec("mouse",
                                                          mouseSpec),
                                      JsSpecs.ofNamedSpec("keyboard",
                                                          keyboardSpec),
                                      JsSpecs.ofNamedSpec("usbHub",
                                                          usbHubSpec)));

    System.out.println(SpecToJsonSchema.convert(peripheralSpec));
    System.out.println(SpecToAvroSchema.convert(peripheralSpec));
  }


}
