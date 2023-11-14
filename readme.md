[![Maven](https://img.shields.io/maven-central/v/com.github.imrafaelmerino/avro-spec/0.1)](https://search.maven.org/artifact/com.github.imrafaelmerino/avro-spec/0.1/jar)

# avro-spec

## Introduction

`avro-spec` empowers you to create schemas and serializers/deserializers
from [specs](https://github.com/imrafaelmerino/json-values#specs). Leveraging the simplicity,
intuitiveness, and composability of creating specs allows you to efficiently define Avro schemas. The provided
serializers/deserializers enable the transmission of immutable and persistent JSON
from [json-values](https://github.com/imrafaelmerino/json-values) through the wire.

## Code Wins Arguments

```code
JsSpec typeEmailSpec = JsEnumBuilder.withName("phone-type")
                                    .withNamespace("example.com")
                                    .build("MOBILE","FIXED");
                                
JsSpec phoneSpec = JsObjSpecBuilder.withName("person-phone")
                                   .withNamespace("example.com")
                                   .build(JsObjSpec.of("number",JsSpecs.str(),
                                                       "type", typeEmailSpec
                                                      )
                                         );

JsSpec emailSpec = JsObjSpecBuilder.withName("person-email")
                                   .withNamespace("example.com")
                                   .build(JsObjSpec.of("address",JsSpecs.str(),
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

**What Can I Do With a Spec?**

- **Parse Strings into JsObj:**
  The parsing is extremely fast because validation occurs during parsing, failing fast if there's an error.

```code

JsObjSpecParser personParser = JsObjSpecParser.of(personSpec);

String personStr = ???;
JsObj person = personParser.parse(personStr);

```

- **Validate JsObj:**
  Validate a JsObj against the spec and get a detailed list of errors, their locations, and causes.

```code

JsObj person = ???;
List<SpecError> errors = personSpec.test(person);

errors.forEach( x -> {
                    System.out.println("Error location: "+ x.path);
                    System.out.println("Invalid value: "+ x.error.value());
                    System.out.println("Error code: " + x.error.code());
               });

```

- **Create Avro Schemas:**
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

- **Create Avro Serializers and Deserializers:**

```code
SpecSerializer serializer = SpecSerializerBuilder.of(recordSpec)
                                                 .build();

SpecDeserializer deserializer = SpecDeserializerBuilder.of(recordSpec)
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

---

## <a name="installation"><a/> Installation

To include avro-spec in your project, add the corresponding dependency to your build tool based on your Java version:

```xml

<dependency>
    <groupId>com.github.imrafaelmerino</groupId>
    <artifactId>avro-spec</artifactId>
    <version>0.1</version>
</dependency>

```

Requires Java 17 or higher

