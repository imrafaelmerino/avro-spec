package jsonvalues.avro;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;
import jio.test.junit.Debugger;
import jsonvalues.JsArray;
import jsonvalues.JsObj;
import jsonvalues.gen.JsArrayGen;
import jsonvalues.gen.JsDoubleGen;
import jsonvalues.gen.JsObjGen;
import jsonvalues.gen.JsStrGen;
import jsonvalues.spec.deserializers.avro.JsArraySpecDeserializer;
import jsonvalues.spec.deserializers.avro.JsArraySpecDeserializerBuilder;
import jsonvalues.spec.deserializers.avro.JsObjSpecDeserializer;
import jsonvalues.spec.deserializers.avro.JsObjSpecDeserializerBuilder;
import jsonvalues.spec.serializers.avro.JsSpecSerializer;
import jsonvalues.spec.serializers.avro.JsSpecSerializerBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * The following schema has been set in the topic "transactions_without_schema" in the kafka cluster.
 * <p>
 * { "namespace": "io.confluent.examples.clients.basicavro", "type": "record", "name": "Payment", "fields": [ {"name":
 * "id", "type": "string"}, {"name": "amount", "type": "double"} ] }
 */
public class PaymentsWithoutSchemaTopicProducerTest {

  @RegisterExtension
  static Debugger debugger =
      MyDebuggers.avroDebugger(Duration.ofSeconds(30));


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
    props.put(ProducerConfig.ACKS_CONFIG,
              "all");
    props.put(ProducerConfig.RETRIES_CONFIG,
              0);
    return new KafkaProducer<>(props);
  }

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


  static String TOPIC = "transactions_without_schema";


  @Test
  public void testDerSerSpecPayment() throws InterruptedException {
    JsSpecSerializer specSerializer =
        JsSpecSerializerBuilder.of(Specs.paymentSpec)
                               .build();

    JsObjSpecDeserializer specDeserializer =
        JsObjSpecDeserializerBuilder.of(Specs.paymentSpec)
                                    .build();
    Supplier<JsObj> gen = JsObjGen.of("id",
                                      JsStrGen.alphabetic(),
                                      "amount",
                                      JsDoubleGen.arbitrary(100.0d,
                                                            1000d)
                                     )
                                  .sample();
    int RECORDS = 10;
    try (var producer = createProducer()) {
      for (long i = 0; i < RECORDS; i++) {
        JsObj payment = gen.get();

        ProducerRecord<String, byte[]> record =
            new ProducerRecord<>(TOPIC,
                                 payment.getStr("id") + i,
                                 specSerializer.serialize(payment));
        producer.send(record);
        Thread.sleep(1000L);
      }

      producer.flush();
      System.out.printf("Successfully produced 10 messages to a topic called %s%n",
                        TOPIC);

    }

    try (var consumer = createConsumer()) {
      consumer.subscribe(List.of(TOPIC));
      int consumed = 0;
      while (true) {
        var records = consumer.poll(Duration.ofMillis(500));
        System.out.println("Consumed " + records.count() + " records.");
        for (var record : records) {
          byte[] bytes = record.value()
                               .get();
          JsObj deserialized = specDeserializer.deserialize(bytes);
          System.out.printf("offset = %d, key = %s, value = %s%n",
                            record.offset(),
                            record.key(),
                            deserialized
                           );

        }
        consumed += records.count();
        if (consumed >= RECORDS) {
          break;
        }
      }
    }
  }

  @Test
  public void testDerSerArrayOfSpecPayment() throws InterruptedException {
    JsSpecSerializer specSerializer =
        JsSpecSerializerBuilder.of(Specs.arrayPaymentSpec)
                               .build();

    JsArraySpecDeserializer specDeserializer =
        JsArraySpecDeserializerBuilder.of(Specs.arrayPaymentSpec)
                                      .build();
    Supplier<JsArray> gen = JsArrayGen.biased(JsObjGen.of("id",
                                                          JsStrGen.alphabetic(),
                                                          "amount",
                                                          JsDoubleGen.arbitrary(100.0d,
                                                                                1000d)
                                                         ),
                                              0,
                                              100)
                                      .sample();
    int RECORDS = 10;
    try (var producer = createProducer()) {
      for (long i = 0; i < RECORDS; i++) {
        JsArray payments = gen.get();

        ProducerRecord<String, byte[]> record =
            new ProducerRecord<>(TOPIC,
                                 payments.size() + "",
                                 specSerializer.serialize(payments));
        producer.send(record);
        Thread.sleep(1000L);
      }

      producer.flush();
      System.out.printf("Successfully produced 10 messages to a topic called %s%n",
                        TOPIC);

    }

    try (var consumer = createConsumer()) {
      consumer.subscribe(List.of(TOPIC));
      int consumed = 0;
      while (true) {
        var records = consumer.poll(Duration.ofMillis(500));
        System.out.println("Consumed " + records.count() + " records.");
        for (var record : records) {
          byte[] bytes = record.value()
                               .get();
          JsArray deserialized = specDeserializer.deserialize(bytes);
          System.out.printf("offset = %d, key = %s, value = %s%n",
                            record.offset(),
                            record.key(),
                            deserialized
                           );

        }
        consumed += records.count();
        if (consumed >= RECORDS) {
          break;
        }
      }
    }
  }

}
