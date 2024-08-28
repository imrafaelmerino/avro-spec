package jsonvalues.avro;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;
import jsonvalues.JsObj;
import jsonvalues.Json;
import jsonvalues.gen.JsDoubleGen;
import jsonvalues.gen.JsObjGen;
import jsonvalues.gen.JsStrGen;
import jsonvalues.spec.JsonToAvro;
import jsonvalues.spec.deserializers.confluent.ConfluentDeserializer;
import jsonvalues.spec.deserializers.confluent.ConfluentObjDeserializer;
import jsonvalues.spec.serializers.confluent.ConfluentSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * The following schema has been set in the topic "payments" in the kafka cluster.
 * <p>
 * { "namespace": "io.confluent.examples.clients.basicavro", "type": "record", "name": "Payment", "fields": [ {"name":
 * "id", "type": "string"}, {"name": "amount", "type": "double"} ] }
 */
@Disabled
public class PaymentsTopicProducerTest {

  final static KafkaProducer<String, GenericRecord> producer = createProducer();
  final static KafkaProducer<String, JsObj> specProducer = createSpecProducer();

  private static KafkaProducer<String, JsObj> createSpecProducer() {

    Properties props = new Properties();
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
              StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
              PaymentSerializer.class);
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

  private static KafkaProducer<String, GenericRecord> createProducer() {
    Properties props = new Properties();
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
              StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
              ConfluentSerializer.class);
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

  private static KafkaConsumer<String, Json<?>> createConsumerWithJsonDeserializer() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
              "localhost:29092");
    props.put(ConsumerConfig.GROUP_ID_CONFIG,
              "group-json-deserializer");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
              StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
              ConfluentDeserializer.class);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
              "earliest");
    props.put(SCHEMA_REGISTRY_URL_CONFIG,
              "http://localhost:8081");
    return new KafkaConsumer<>(props);
  }

  private static KafkaConsumer<String, JsObj> createConsumerWithJsSpecDeserializer() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
              "localhost:29092");
    props.put(ConsumerConfig.GROUP_ID_CONFIG,
              "group-json-spec-deserializer");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
              StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
              PaymentDeserializer.class);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
              "earliest");
    props.put(SCHEMA_REGISTRY_URL_CONFIG,
              "http://localhost:8081");
    return new KafkaConsumer<>(props);
  }

  private static KafkaConsumer<String, JsObj> createConsumerWithJsonObjDeserializer() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
              "localhost:29092");
    props.put(ConsumerConfig.GROUP_ID_CONFIG,
              "group-jsobj-deserializer");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
              StringDeserializer.class);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
              "earliest");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
              ConfluentObjDeserializer.class);
    props.put(SCHEMA_REGISTRY_URL_CONFIG,
              "http://localhost:8081");
    return new KafkaConsumer<>(props);
  }

  static String TOPIC = "transactions";

  @Test
  public void testCreateMessagesWithSpecProducer() throws InterruptedException {
    Supplier<JsObj> gen = JsObjGen.of("id",
                                      JsStrGen.alphabetic(),
                                      "amount",
                                      JsDoubleGen.arbitrary(100.0d,
                                                            1000d)
                                     )
                                  .sample();
    int RECORDS = 10;
    try (specProducer) {
      for (long i = 0; i < RECORDS; i++) {
        final JsObj payment = gen.get();
        final ProducerRecord<String, JsObj> record =
            new ProducerRecord<>(TOPIC,
                                 payment.getStr("id") + i,
                                 payment);
        var unused = specProducer.send(record);
        Thread.sleep(1000L);
      }

      producer.flush();
      System.out.printf("Successfully produced 10 messages to a topic called %s%n",
                        TOPIC);

    }

    try (var consumer = createConsumerWithJsSpecDeserializer()) {
      consumer.subscribe(List.of(TOPIC));
      int consumed = 0;
      while (true) {
        var records = consumer.poll(Duration.ofMillis(500));
        System.out.println("Consumed " + records.count() + " records.");
        for (var record : records) {
          System.out.printf("offset = %d, key = %s, value = %s%n",
                            record.offset(),
                            record.key(),
                            record.value()
                                  .toPrettyString()
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
  public void testCreateMessages() throws InterruptedException {
    Supplier<JsObj> gen = JsObjGen.of("id",
                                      JsStrGen.alphabetic(),
                                      "amount",
                                      JsDoubleGen.arbitrary(100.0d,
                                                            1000d)
                                     )
                                  .sample();
    int RECORDS = 10;
    try (producer) {
      for (long i = 0; i < RECORDS; i++) {
        final JsObj payment = gen.get();
        final ProducerRecord<String, GenericRecord> record =
            new ProducerRecord<>(TOPIC,
                                 payment.getStr("id") + i,
                                 JsonToAvro.convert(payment,
                                                    Specs.paymentSpec));
        var unused = producer.send(record);
        Thread.sleep(1000L);
      }

      producer.flush();
      System.out.printf("Successfully produced 10 messages to a topic called %s%n",
                        TOPIC);

    }

    try (var consumer = createConsumerWithJsonDeserializer()) {
      consumer.subscribe(List.of(TOPIC));
      int consumed = 0;
      while (true) {
        var records = consumer.poll(Duration.ofMillis(500));
        System.out.println("Consumed " + records.count() + " records.");
        for (var record : records) {
          System.out.printf("offset = %d, key = %s, value = %s%n",
                            record.offset(),
                            record.key(),
                            record.value()
                           );
        }
        consumed += records.count();
        if (consumed >= RECORDS) {
          break;
        }
      }
    }
    try (var consumer = createConsumerWithJsonObjDeserializer()) {
      consumer.subscribe(List.of(TOPIC));
      int consumed = 0;
      while (true) {
        ConsumerRecords<String, JsObj> records = consumer.poll(Duration.ofMillis(500));
        System.out.println("Consumed " + records.count() + " records.");
        for (var record : records) {
          System.out.printf("offset = %d, key = %s, value = %s%n",
                            record.offset(),
                            record.key(),
                            record.value()
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
