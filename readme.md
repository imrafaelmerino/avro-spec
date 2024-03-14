[![Maven](https://img.shields.io/maven-central/v/com.github.imrafaelmerino/avro-spec/0.5)](https://search.maven.org/artifact/com.github.imrafaelmerino/avro-spec/0.5/jar)

- [Avro spec](#avro-spec)
- [Avro schemas](#avro-schema)
- [A more elaborated example with recursive schemas](#recursive-schema)
- [Avro serializer and deserializer](#seria-deseria)
- [Installation](#installation)

## <a name="avro-spec"><a/> Avro spec

`avro-spec` empowers you to create [Avro](https://avro.apache.org/) schemas and
serializers/deserializers with the [specs](https://github.com/imrafaelmerino/json-values#specs) from
[json-values](https://github.com/imrafaelmerino/json-values). Leveraging the simplicity,
intuitiveness, and composability of creating specs allows you to efficiently define Avro schemas.
The provided serializers/deserializers enable the transmission of the immutable and persistent JSON
from [json-values](https://github.com/imrafaelmerino/json-values) through the wire in Avro format,
supporting Confluent Schema Registry.

## <a name="avro-schema"><a/> Avro schemas

Create Avro schemas in a natural way, for example, from the `personSpec`:

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
                "symbols": ["MOBILE", "FIXED"]
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

## <a name="recursive-schema"><a/> More elaborated example with recursive schemas

The [json-values](https://github.com/imrafaelmerino/json-values/) library simplifies the
implementation of inheritance and the generation of structured data in Java. Let's explore an
example showcasing the ease of defining object specifications and its Avro schema, generating data,
and validating against specifications.

In this example, picked from [this
article](https://json-schema.org/blog/posts/modelling-inheritance#so-is-inheritance-in-json-schema-possible)
we model a hierarchy of devices, including mice, keyboards, and USB hubs. Each device type has
specific attributes, and we use inheritance to share common fields across all device types.

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

```json
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
          "symbols": ["ball", "optical"]
        }
      },
      {
        "name": "type",
        "type": {
          "type": "enum",
          "name": "type",
          "symbols": ["mouse", "keyboard", "usb_hub"]
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
            "items": ["mouse", "keyboard", "usb_hub"]
          }
        ],
        "default": null
      }
    ]
  }
]
```

## <a name="serializers"><a/> Avro serializers and deserializers

Avro serializers and deserializers play a crucial role in efficiently encoding and decoding data for
communication between Kafka producers and consumers. Here, we explore different options available
for serialization and deserialization with Avro, including integration with the Confluent Schema
Registry.

- Confluent Avro Serializers with Schema Registry Integration

  - `jsonvalues.spec.serializers.confluent.avro.GenericContainerSerializer`: Serializes an Avro
    `GenericContainer` into bytes.
  - `jsonvalues.spec.serializers.confluent.avro.JsSpecSerializer`: Serializes a JSON object
    conforming to a spec into bytes.

- Avro serializers:
  - `jsonvalues.spec.serializers.avro.JsSpecSerializer`: Serializes a JSON object conforming to a
    spec into bytes.

And the following deserializers:

- Confluent Avro Deserializers with Schema Registry Integration

  - `jsonvalues.spec.deserializers.confluent.avro.JsObjDeserializer`: Deserializes bytes into a
    JsObj.
  - `jsonvalues.spec.deserializers.confluent.avro.JsArrayDeserializer`: Deserializes bytes into a
    JsArray.
  - `jsonvalues.spec.deserializers.confluent.avro.JsDeserializer`: Deserializes bytes into a JSON
    object.
  - `jsonvalues.spec.deserializers.confluent.avro.JsObjSpecDeserializer`: Deserializes bytes into a
    JsObj conforming to a spec.
  - `jsonvalues.spec.deserializers.confluent.avro.JsArraySpecDeserializer`: Deserializes bytes into
    a JsArray conforming to a spec.

- Avro deserializers:
  - `jsonvalues.spec.deserializers.avro.JsObjSpecDeserializer`: Deserializes bytes into a JsObj
    conforming to a spec.
  - `jsonvalues.spec.deserializers.avro.JsArraySpecDeserializer`: Deserializes bytes into a JsArray
    conforming to a spec.

**Which serializer to use?**

Let's consider the following example where we have a topic named "payments":

```
 String TOPIC = "payments";

 JsObjSpec paymentSpec =
    JsObjSpecBuilder.withName("Payment")
                    .withNamespace("examples.avro.spec")
                    .build(JsObjSpec.of("id", JsSpecs.str(),
                                        "amount", JsSpecs.doubleNumber()
                                       )
                          );

 Supplier<JsObj> paymentGen =
    JsObjGen.of("id", JsStrGen.alphabetic(),
                "amount", JsDoubleGen.arbitrary(100.0d,1000d)).sample();

```

When working with Kafka, using a single producer for all topics is highly recommended for optimal
performance due to its thread safety and batching capabilities. Given that, you have two options:

1. `jsonvalues.spec.confluent.avro.GenericContainerSerializer` for Avro Format with Schema Registry
   Integration:

```code
 private static KafkaProducer<String, GenericRecord> createProducer() {
    Properties props = new Properties();
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
              StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
              GenericContainerSerializer.class);
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
              "localhost:29092");
    props.put(SCHEMA_REGISTRY_URL_CONFIG,
              "http://localhost:8081");
    return new KafkaProducer<>(props);
  }


 int RECORDS = 10;
 try (var producer = createProducer()) {
    for (long i = 0; i < RECORDS; i++)
    {
       JsObj payment = paymentGen.get();

       GenericRecord record =  JsonToAvro.convert(payment,paymentSpec)

       ProducerRecord<String, GenericRecord> record =
            new ProducerRecord<>(TOPIC, payment.getStr("id") + i, record);

       producer.send(record);
       Thread.sleep(1000L);
   }

   producer.flush();
   System.out.printf("Successfully produced 10 messages to a topic called %s%n",
                     TOPIC
                    );

 }

```

In the first case, we're using `jsonvalues.spec.confluent.avro.GenericContainerSerializer` as the
value serializer for the producer. Before sending the data to the Kafka topic, we need to convert
the JSON object (`payment`) into the Avro object `GenericRecord` using
`JsonToAvro.convert(payment, paymentSpec)`.

2. `jsonvalues.spec.avro.JsSpecSerializer` for Avro Format without Schema Registry:

```code
 private static KafkaProducer<String, byte[]> createProducer() {
    Properties props = new Properties();
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
              StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
              ByteArraySerializer.class);
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
              "localhost:29092");
    props.put(SCHEMA_REGISTRY_URL_CONFIG,
              "http://localhost:8081");
    return new KafkaProducer<>(props);
 }

 JsSpecSerializer paymentSerializer =
        JsSpecSerializerBuilder.of(paymentSpec)
                               .build();

 int RECORDS = 10;
 try (var producer = createProducer()) {
   for (long i = 0; i < RECORDS; i++)
   {
       JsObj payment = paymentGen.get();

       ProducerRecord<String, byte[]> record =
            new ProducerRecord<>(TOPIC,
                                 payment.getStr("id") + i,
                                 paymentSerializer.serialize(payment));
       producer.send(record);
       Thread.sleep(1000L);
   }

   producer.flush();
   System.out.printf("Successfully produced 10 messages to a topic called %s%n",
                     TOPIC);

  }

```

In this second case, we're utilizing `org.apache.kafka.common.serialization.ByteArraySerializer`
from Kafka as the value serializer for the producer. Additionally, we're employing the created
paymentSerializer to convert the JSON object into bytes in avro format before sending it to the
Kafka topic.

If you have one specific producer for a topic, because the topic requires a specific configuration:

1. `jsonvalues.spec.confluent.avro.JsSpecSerializer` for Avro format and integration with Confluent
   Schema Registry In this case you need to create a new class and extend `JsSpecSerializer`
   providing the spec. Find below and example:

```code

public final class PaymentSerializer extends JsSpecSerializer {

  @Override
  protected boolean isJFREnabled() {
    return true;
  }

  @Override
  protected JsSpec getSpec() {
    return paymentSpec;
  }
}

private static KafkaProducer<String, JsObj> createPaymentProducer() {

    Properties props = new Properties();
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
              StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
              PaymentSerializer.class);
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
              "localhost:29092");
    props.put(SCHEMA_REGISTRY_URL_CONFIG,
              "http://localhost:8081");
    return new KafkaProducer<>(props);
  }

 try (var producer = createPaymentProducer()) {
    for (long i = 0; i < RECORDS; i++)
    {
        JsObj payment = gen.get();
        ProducerRecord<String, JsObj> record =
            new ProducerRecord<>(TOPIC,
                                 payment.getStr("id") + i,
                                 payment);
        producer.send(record);
        Thread.sleep(1000L);
      }

      producer.flush();
      System.out.printf("Successfully produced 10 messages to a topic called %s%n",
                        TOPIC);

    }
```

In this setup, you create a new class `PaymentSerializer` by extending `JsSpecSerializer`, where you
override the `getSpec()` method to provide the necessary spec (`paymentSpec`). Then, when creating
the Kafka producer (`createPaymentProducer()`), you specify `PaymentSerializer.class` as the value
serializer to use for serialization. This serializer will automatically handle the serialization
process when you pass a `JsObj` to the producer, simplifying the process for you.

2. `jsonvalues.spec.avro.JsSpecSerializer` for Avro format without Schema registry. In this case, we
   use the `org.apache.kafka.common.serialization.ByteArraySerializer` from Kafka as the value
   serializer for the producer. Additionally, we employ the `JsSpecSerializer` created using a
   builder to convert JSON objects into bytes in Avro format before sending them to the Kafka topic.

```code
private static KafkaProducer<String, byte[]> createProducer() {
    Properties props = new Properties();
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
              StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
              ByteArraySerializer.class);
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
              "localhost:29092");
    props.put(SCHEMA_REGISTRY_URL_CONFIG,
              "http://localhost:8081");
    return new KafkaProducer<>(props);
  }

JsSpecSerializer paymentSerializer =
        JsSpecSerializerBuilder.of(Specs.paymentSpec)
                               .build();


int RECORDS = 10;
try (var producer = createProducer())
    {
      for (long i = 0; i < RECORDS; i++) {
        JsObj payment = paymentGen.get();

        ProducerRecord<String, byte[]> record =
            new ProducerRecord<>(TOPIC,
                                 payment.getStr("id") + i,
                                 paymentSerializer.serialize(payment));
        producer.send(record);
        Thread.sleep(1000L);
      }

      producer.flush();
      System.out.printf("Successfully produced 10 messages to a topic called %s%n",
                        TOPIC);

    }


```

What about deserializers?

1. If you opt for the Confluent Avro serializer, you have several deserialization options integrated
   with the Confluent Schema Registry:

- `jsonvalues.spec.confluent.avro.JsObjDeserializer`: Deserializes into a JsObj.
- `jsonvalues.spec.confluent.avro.JsArrayDeserializer`: Deserializes into a JsArray.
- `jsonvalues.spec.confluent.avro.JsDeserializer`: Deserializes into a Json (can be either a JsObj
  or a JsArray).
- You can also create custom deserializers by extending
  `jsonvalues.spec.confluent.avro.JsObjSpecDeserializer` or
  `jsonvalues.spec.confluent.avro.JsArraySpecDeserializer` to ensure deserialized data conforms to a
  specific schema.

Example using `JsObjDeserializer`:

```code

private static KafkaConsumer<String, JsObj> createPaymentDeserializer() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
              "localhost:29092");
    props.put(ConsumerConfig.GROUP_ID_CONFIG,
              "my-consumer-group");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
              StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
              jsonvalues.spec.confluent.avro.JsObjDeserializer.class);
    props.put(SCHEMA_REGISTRY_URL_CONFIG,
              "http://localhost:8081");
    return new KafkaConsumer<>(props);
  }


try (var consumer = createConsumerWithJsonObjDeserializer()) {
      consumer.subscribe(List.of(TOPIC));
      while (true) {
        ConsumerRecords<String, JsObj> records = consumer.poll(Duration.ofMillis(500));
        System.out.println("Consumed " + records.count() + " records.");
        for (var record : records) {
          JsObj obj = record.value();
          System.out.printf("offset = %d, key = %s, value = %s%n",
                            record.offset(),
                            record.key(),
                            obj
                           );
        }
      }
    }

```

Example using a spec deserializer:

```code

//must create a new deserializer extending JsObjSpecDeserializer
public final class PaymentDeserializer extends JsObjSpecDeserializer {

  @Override
  protected JsSpec getSpec() {
    return paymentSpec;
  }

  @Override
  protected boolean isJFREnabled() {
    return true;
  }
}

private static KafkaConsumer<String, JsObj> createPaymentDeserializer() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
              "localhost:29092");
    props.put(ConsumerConfig.GROUP_ID_CONFIG,
              "my-consumer-group");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
              StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
              PaymentDeserializer.class);
    props.put(SCHEMA_REGISTRY_URL_CONFIG,
              "http://localhost:8081");
    return new KafkaConsumer<>(props);
}


// same code as before


```

2. If you're utilizing the Avro serializer without Schema Registry integration, you have the option
   to employ the builders `jsonvalues.spec.avro.JsObjSpecDeserializerBuilder` and
   `jsonvalues.spec.avro.JsArraySpecDeserializerBuilder` to construct deserializers from
   specifications. These builders facilitate the creation of custom deserializers tailored to your
   specific data schemas.

Below is an example demonstrating the usage of a ByteArrayDeserializer from Kafka along with a spec
deserializer to convert bytes into a JsObj and consume a Kafka topic:

```code

  private static KafkaConsumer<String, Bytes> createConsumer() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
              "localhost:29092");
    props.put(ConsumerConfig.GROUP_ID_CONFIG,
              "group-json-deserializer-1");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
              StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
              BytesDeserializer.class);
    props.put(SCHEMA_REGISTRY_URL_CONFIG,
              "http://localhost:8081");
    return new KafkaConsumer<>(props);
  }

  JsObjSpecDeserializer paymentDeserializer =
        JsObjSpecDeserializerBuilder.of(Specs.paymentSpec)
                                    .build();

  try (var consumer = createConsumer()) {
      consumer.subscribe(List.of(TOPIC));
      while (true) {
        ConsumerRecords<String, JsObj> records = consumer.poll(Duration.ofMillis(500));
        System.out.println("Consumed " + records.count() + " records.");
        for (var record : records) {
          Bytes kakfaBytes = record.value();
          byte[] bytes = kakfaBytes.get();
          JsObj obj = paymentDeserializer.deserialize(bytes);
          System.out.printf("offset = %d, key = %s, value = %s%n",
                            record.offset(),
                            record.key(),
                            obj
                           );
        }
      }
    }


```

**Monitoring Serializers/Deserializers with JFR Events**

All the serializers and deserializers in this library support Java Flight Recorder (JFR) events.

## <a name="installation"><a/> Installation

To include avro-spec in your project, add the corresponding dependency to your build tool based on
your Java version:

```xml

<dependency>
  <groupId>com.github.imrafaelmerino</groupId>
  <artifactId>avro-spec</artifactId>
  <version>0.5</version>
</dependency>

```

Requires Java 21 or higher

Find [here](./changelog.md) the releases notes.
