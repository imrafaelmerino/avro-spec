[![Maven](https://img.shields.io/maven-central/v/com.github.imrafaelmerino/avro-spec/0.3)](https://search.maven.org/artifact/com.github.imrafaelmerino/avro-spec/0.3/jar)

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

### Integration with Jio-Test for Real-time Debugging

The Avro-spec library seamlessly integrates with [Jio-Test](https://github.com/imrafaelmerino/JIO#jio-test), providing a
convenient
way to enable real-time debugging during testing. The `Debugger` class from Jio-Test is utilized to configure Java
Flight Recorder (JFR) events for Avro serialization and deserialization.

### Usage Example

```code
import jio.test.junit.Debugger;
import org.junit.jupiter.api.extension.RegisterExtension;
import jsonvalues.spec.SpecDeserializerDebugger;
import jsonvalues.spec.SpecSerializerDebugger;

public class AvroSpecIntegrationTest {

    @RegisterExtension
    static Debugger debugger = avroDebugger(Duration.ofSeconds(2));

    // Your test methods involving Avro serialization/deserialization go here

    private static Debugger avroDebugger(Duration duration) {
        Debugger debugger = Debugger.of(duration);
        debugger.addEventConsumer("AvroSpecDeserializerEvent", 
                                  SpecDeserializerDebugger.INSTANCE);
        debugger.addEventConsumer("AvroSpecSerializerEvent",
                                  SpecSerializerDebugger.INSTANCE);
        return debugger;
    }
    
    @Test
    public void test(){
    
      ???
    
    }
}

```

By simply integrating the `Debugger` extension from Jio-Test, you gain real-time insights into the Avro serialization
and deserialization events. During the execution of tests involving JSON objects from json-values, the console output
will provide detailed information about the serialization process. This includes the name of the serializer, the
result (success or failure), the number of errors encountered, the number of successful operations, the duration of the
operation, any exceptions that occurred, the thread involved, and the timestamp of the event.

```text


event: avro-serialization, serializer: person-serializer, result: SUCCESS, #errors: 0, #success: 5,
duration: 360,083 µs, exception: , thread: main, event-start-time: 2023-11-15T14:34:17.88513825+01:00

event: avro-deserialization, deserializer: person-deserializer, result: SUCCESS, #errors: 0, #success: 5,
duration: 294,875 µs, exception: , thread: main, event-start-time: 2023-11-15T14:34:17.885509458+01:00

event: avro-serialization, serializer: person-serializer, result: SUCCESS, #errors: 0, #success: 6,
duration: 306,083 µs, exception: , thread: main, event-start-time: 2023-11-15T14:34:17.885926792+01:00



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
    <version>0.3</version>
</dependency>

```

Requires Java 17 or higher

