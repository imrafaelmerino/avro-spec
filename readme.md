[![Maven](https://img.shields.io/maven-central/v/com.github.imrafaelmerino/avro-spec/0.2)](https://search.maven.org/artifact/com.github.imrafaelmerino/avro-spec/0.2/jar)

- [Avro spec](#avro-spec)
- [Code Wins Arguments](#cwa)
- [Avro schemas](#avro-schema)
- [Avro serializer and deserializer](#seria-deseria)
- [What else can i do with a spec?](#specs)
- [Installation](#installation)

## <a name="avro-spec"><a/> Avro spec

`avro-spec` empowers you to create [Avro](https://avro.apache.org/) schemas and serializers/deserializers
with the [specs](https://github.com/imrafaelmerino/json-values#specs) from json-values. Leveraging the simplicity,
intuitiveness, and composability of creating specs allows you to efficiently define Avro schemas. The provided
serializers/deserializers enable the transmission of immutable and persistent JSON
from [json-values](https://github.com/imrafaelmerino/json-values) through the wire.

## <a name="cwa"><a/> Code Wins Arguments

```code
JsSpec typeEmailSpec = JsEnumBuilder.withName("phone_type")
                                    .withNamespace("example.com")
                                    .build("MOBILE","FIXED");
                                
JsSpec phoneSpec = JsObjSpecBuilder.withName("person_phone")
                                   .withNamespace("example.com")
                                   .build(JsObjSpec.of("number", JsSpecs.str(),
                                                       "type", typeEmailSpec
                                                      )
                                         );

JsSpec emailSpec = JsObjSpecBuilder.withName("person_email")
                                   .withNamespace("example.com")
                                   .build(JsObjSpec.of("address", JsSpecs.str(),
                                                       "verified", JsSpecs.bool()
                                                      )
                                         );
                                         
JsSpec contactSpec = JsSpecs.oneSpecOf(phoneSpec, emailSpec);                                         
                                         

JsObjSpec personSpec = 
    JsObjSpecBuilder.withName("person")
                    .withNamespace("example.com")
                    .build(JsObjSpec.of("name", JsSpecs.str(),
                                        "age", JsSpecs.integer(n -> n > 16),
                                        "surname", JsSpecs.str(),
                                        "contact", contactSpec
                                       )
                          );
```

## <a name="avro-schema"><a/> Avro schemas

Create Avro schemas in a natural way, for example, from the `personSpec`:

```code

Schema schema = SpecToSchema.convert(personSpec);

System.out.println(schema);

```

Resulting Schema:

```json

{
  "type": "record",
  "name": "person",
  "namespace": "example.com",
  "fields": [
    {
      "name": "name",
      "type": "string"
    },
    {
      "name": "age",
      "type": "int"
    },
    {
      "name": "surname",
      "type": "string"
    },
    {
      "name": "contact",
      "type": [
        {
          "type": "record",
          "name": "person_phone",
          "fields": [
            {
              "name": "number",
              "type": "string"
            },
            {
              "name": "type",
              "type": {
                "type": "enum",
                "name": "phone_type",
                "symbols": [
                  "MOBILE",
                  "FIXED"
                ]
              }
            }
          ]
        },
        {
          "type": "record",
          "name": "person_email",
          "fields": [
            {
              "name": "address",
              "type": "string"
            },
            {
              "name": "verified",
              "type": "boolean"
            }
          ]
        }
      ]
    }
  ]
}

```

## <a name="seria-deseria"><a/> Avro serializer and deserializer

```code

SpecSerializer serializer = SpecSerializerBuilder.of(personSpec)
                                                 .build();

SpecDeserializer deserializer = SpecDeserializerBuilder.of(personSpec)
                                                       .build();
                                                       
                
JsObj person = ???                                                      
Assertions.assertEquals(person,
                        deserializer.binaryDecode(serializer.binaryEncode(person)));                                                       

Assertions.assertEquals(person, 
                        deserializer.jsonDecode(serializer.jsonEncode(person)));                                                       

```

If the person object conforms to the spec, you are good. During testing, if the object doesn't conform to the spec,
you'll get an assertion error, freeing you from checking this in every test you write. The serializer and deserializer
builder have more options to define an `EncoderFactory` different than the default one or to specify `BinaryEncoder` to
be reused.

**Monitoring Avro Spec Deserialization with JFR Events**

When debugging is enabled for the Avro Spec Deserializer, the library emits Java Flight Recorder (JFR) events to provide
insights into the deserialization process. These events, captured by the `AvroSpecDeserializerEvent`, include details
about successful deserializations, encountered errors, and associated exception details. Counters keep track of the
overall success and error counts, aiding in monitoring and performance analysis. Integrating JFR events provides a
valuable tool for developers to gain visibility into the Avro spec deserialization workflow and diagnose issues
effectively.

```code

SpecDeserializer deserializer = 
    SpecDeserializerBuilder.of(personSpec)
                           .enableDebug("person-deserializer")
                           .build();

```

**Enabling Debugging for Avro Spec Serialization**

The Avro Spec Serializer offers a built-in debugging feature that, when enabled, utilizes Java Flight Recorder (JFR)
events to provide insights into the serialization process. By invoking the `enableDebug` method with a specified
serializer name, the library generates events captured by the `AvroSpecSerializerEvent` class. These events furnish
information about both successful serializations and encountered errors, including details of any exceptions that may
have occurred. Utilizing counters for overall success and error counts, these events become valuable tools for
monitoring and diagnosing issues during the Avro spec serialization workflow.

```code

SpecSerializer serializer = 
    SpecSerializerBuilder.of(personSpec)
                         .enableDebug("person-serializer")
                         .build();

```

## <a name="specs"><a/> What else can I do with a spec?

**Parsing strings into Json:**

The parsing is extremely fast because validation occurs during parsing, failing fast if there's an error.

```code

JsObjSpecParser personParser = JsObjSpecParser.of(personSpec);

String personStr = ???;
JsObj person = personParser.parse(personStr);

```

**Validate JsObj:**

Validate a JsObj against the spec and get a detailed list of errors, their locations, and causes.

```code

JsObj person = ???;
List<SpecError> errors = personSpec.test(person);

errors.forEach( x -> {
                    System.out.println("Error location: " + x.path);
                    System.out.println("Invalid value: " + x.error.value());
                    System.out.println("Error code: " + x.error.code());
               });

```

## <a name="installation"><a/> Installation

To include avro-spec in your project, add the corresponding dependency to your build tool based on your Java version:

```xml

<dependency>
    <groupId>com.github.imrafaelmerino</groupId>
    <artifactId>avro-spec</artifactId>
    <version>0.2</version>
</dependency>

```

Requires Java 17 or higher

