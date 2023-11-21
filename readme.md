[![Maven](https://img.shields.io/maven-central/v/com.github.imrafaelmerino/avro-spec/0.4)](https://search.maven.org/artifact/com.github.imrafaelmerino/avro-spec/0.4/jar)

- [Avro spec](#avro-spec)
- [Code Wins Arguments](#cwa)
- [Avro schemas](#avro-schema)
- [Avro serializer and deserializer](#seria-deseria)
- [A more elaborated example with recursive schemas](#recursive-schema)
- [What else can i do with a spec?](#specs)
- [Installation](#installation)

## <a name="avro-spec"><a/> Avro spec

`avro-spec` empowers you to create [Avro](https://avro.apache.org/) schemas and serializers/deserializers
with the [specs](https://github.com/imrafaelmerino/json-values#specs) from json-values. Leveraging the simplicity,
intuitiveness, and composability of creating specs allows you to efficiently define Avro schemas. The provided
serializers/deserializers enable the transmission of the immutable and persistent JSON
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

If the person JSON conforms to the spec, you are good. During testing, if the object doesn't conform to the spec,
you'll get an assertion error, freeing you from checking this in every test you write. The serializer and deserializer
builder have more options to define an `EncoderFactory` different than the default one or to specify `BinaryEncoder` to
be reused.

## <a name="recursive-schema"><a/> More elaborated example with recursive schemas

The [json-values](https://github.com/imrafaelmerino/json-values/) library simplifies the implementation of inheritance
and the generation of structured data in Java. Let's explore an example showcasing the ease of defining object specifications and its Avro schema, generating data, and
validating against specifications.

In this example, picked
from [this article](https://json-schema.org/blog/posts/modelling-inheritance#so-is-inheritance-in-json-schema-possible)
we model a hierarchy of devices, including mice, keyboards, and USB hubs. Each device type has specific
attributes, and we use inheritance to share common fields across all device types.

```code 
String NAME_FIELD = "name";
String TYPE_FIELD = "type";
String BUTTON_COUNT_FIELD = "buttonCount";
String WHEEL_COUNT_FIELD = "wheelCount";
String TRACKING_TYPE_FIELD = "trackingType";
String KEY_COUNT_FIELD = "keyCount";
String MEDIA_BUTTONS_FIELD = "mediaButtons";
String CONNECTED_DEVICES_FIELD = "connectedDevices";
String PERIPHERAL_FIELD = "peripheral";
List<String> TRACKING_TYPE_ENUM = List.of("ball", "optical");

var baseSpec = 
  JsObjSpec.of(NAME_FIELD, JsSpecs.str(),
              TYPE_FIELD, JsEnumBuilder.withName("type")
                                       .build("mouse", "keyboard", "usb_hub")
              );

var baseGen = JsObjGen.of(NAME_FIELD, JsStrGen.alphabetic());

var mouseSpec =
 JsObjSpecBuilder.withName("mouse")
                 .build(JsObjSpec.of(BUTTON_COUNT_FIELD, JsSpecs.integer(),
                                     WHEEL_COUNT_FIELD, JsSpecs.integer(),
                                     TRACKING_TYPE_FIELD, 
                                     JsEnumBuilder.withName("tracking_type")
                                                  .build(TRACKING_TYPE_ENUM)
                                     )
                                 .concat(baseSpec)
                        );

var mouseGen =
 JsObjGen.of(BUTTON_COUNT_FIELD, JsIntGen.arbitrary(0, 10),
             WHEEL_COUNT_FIELD, JsIntGen.arbitrary(0, 10),
             TRACKING_TYPE_FIELD, Combinators.oneOf(TRACKING_TYPE_ENUM)
                                             .map(JsStr::of),
             TYPE_FIELD, Gen.cons(JsStr.of("mouse"))
            )
         .concat(baseGen);

var keyboardSpec =
 JsObjSpecBuilder.withName("keyboard")
                 .build(JsObjSpec.of(KEY_COUNT_FIELD, JsSpecs.integer(),
                                     MEDIA_BUTTONS_FIELD, JsSpecs.bool()
                                     )
                                 .concat(baseSpec)    
                       );

var keyboardGen =
  JsObjGen.of(KEY_COUNT_FIELD, JsIntGen.arbitrary(0, 10),
              MEDIA_BUTTONS_FIELD, JsBoolGen.arbitrary(),
              TYPE_FIELD, Gen.cons(JsStr.of("keyboard"))
             )
          .concat(baseGen);


var usbHubSpec =
  JsObjSpecBuilder.withName("usb_hub")
                  .withFieldsDefaults(Map.of(CONNECTED_DEVICES_FIELD, JsNull.NULL))
                  .build(JsObjSpec.of(CONNECTED_DEVICES_FIELD,
                                      arrayOfSpec(JsSpecs.ofNamedSpec(PERIPHERAL_FIELD)).nullable()
                                      )
                                   .withOptKeys(CONNECTED_DEVICES_FIELD)
                                   .concat(baseSpec)
                        );

var usbHubGen =
  JsObjGen.of(CONNECTED_DEVICES_FIELD,
              JsArrayGen.biased(NamedGen.of(PERIPHERAL_FIELD), 2, 10),
              TYPE_FIELD, Gen.cons(JsStr.of("usb_hub"))
             )
           .withOptKeys(CONNECTED_DEVICES_FIELD)
           .concat(baseGen);


var peripheralSpec =
  JsSpecs.ofNamedSpec(PERIPHERAL_FIELD,
                      oneSpecOf(mouseSpec,
                                keyboardSpec,
                                usbHubSpec
                                )
                      );

var peripheralGen =
   NamedGen.of(PERIPHERAL_FIELD,
               Combinators.oneOf(mouseGen,
                                 keyboardGen,
                                 usbHubGen
                                 )
              );

Schema schema = SpecToSchema.convert(peripheralSpec);

System.out.println(schema);

SpecSerializer serializer =
   SpecSerializerBuilder.of(peripheralSpec)
                        .enableDebug("peripheral-serializer")
                        .build();
  
SpecDeserializer deserializer =
   SpecDeserializerBuilder.of(peripheralSpec, peripheralSpec)
                          .enableDebug("peripheral-deserializer")
                          .build();

peripheralGen.sample(10)
             .forEach(obj -> {

                              byte[] serialized = serializer.binaryEncode(obj);

                              JsObj deserialized = deserializer.binaryDecode(serialized);
                              
                              Assertions.assertEquals(obj,
                                                      deserialized.filterValues(JsValue::isNotNull())
                                                     );

                              }
                      );


```

and the Avro schema would be:

````json

[
  {
    "type": "record",
    "name": "mouse",
    "fields": [
      {
        "name": "wheelCount",
        "type": "int"
      },
      {
        "name": "name",
        "type": "string"
      },
      {
        "name": "buttonCount",
        "type": "int"
      },
      {
        "name": "trackingType",
        "type": {
          "type": "enum",
          "name": "tracking_type",
          "symbols": [
            "ball",
            "optical"
          ]
        }
      },
      {
        "name": "type",
        "type": {
          "type": "enum",
          "name": "type",
          "symbols": [
            "mouse",
            "keyboard",
            "usb_hub"
          ]
        }
      }
    ]
  },
  {
    "type": "record",
    "name": "keyboard",
    "fields": [
      {
        "name": "name",
        "type": "string"
      },
      {
        "name": "keyCount",
        "type": "int"
      },
      {
        "name": "mediaButtons",
        "type": "boolean"
      },
      {
        "name": "type",
        "type": "type"
      }
    ]
  },
  {
    "type": "record",
    "name": "usb_hub",
    "fields": [
      {
        "name": "name",
        "type": "string"
      },
      {
        "name": "type",
        "type": "type"
      },
      {
        "name": "connectedDevices",
        "type": [
          "null",
          {
            "type": "array",
            "items": [
              "mouse",
              "keyboard",
              "usb_hub"
            ]
          }
        ],
        "default": null
      }
    ]
  }
]


````

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
convenient way to enable real-time debugging during testing. The `Debugger` class from Jio-Test is utilized to configure Java
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

    private static Debugger avroDebugger(Duration duration) {
        Debugger debugger = Debugger.of(duration);
        debugger.addEventConsumer("AvroSpecDeserializerEvent", 
                                  SpecDeserializerDebugger.INSTANCE);
        debugger.addEventConsumer("AvroSpecSerializerEvent",
                                  SpecSerializerDebugger.INSTANCE);
        return debugger;
    }

    // Your test methods involving Avro serialization/deserialization go here

    
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
    <version>0.4</version>
</dependency>

```

Requires Java 17 or higher

Find [here](./changelog.md) the releases notes.
