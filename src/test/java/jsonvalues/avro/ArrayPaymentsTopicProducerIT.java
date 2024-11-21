package jsonvalues.avro;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;
import jsonvalues.JsArray;
import jsonvalues.Json;
import jsonvalues.gen.JsArrayGen;
import jsonvalues.gen.JsDoubleGen;
import jsonvalues.gen.JsObjGen;
import jsonvalues.gen.JsStrGen;
import jsonvalues.spec.JsonToAvro;
import jsonvalues.spec.deserializers.confluent.ConfluentArrayDeserializer;
import jsonvalues.spec.deserializers.confluent.ConfluentDeserializer;
import jsonvalues.spec.serializers.confluent.ConfluentSerializer;
import org.apache.avro.generic.GenericArray;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.apache.kafka.common.serialization.VoidSerializer;
import org.junit.jupiter.api.Test;
/**
 *
 * The following schema has been set in the topic "payments" in the kafka cluster.
 * <p>
 * { "namespace": "io.confluent.examples.clients.basicavro", "type": "record", "name": "Payment", "fields": [ {"name":
 * "id", "type": "string"}, {"name": "amount", "type": "double"} ] }
 */

public class ArrayPaymentsTopicProducerIT {

  final static KafkaProducer<Void, GenericArray<?>> producer = createProducer();
  private static final String BOOSTRAP_SERVER = "localhost:9092";
  private static final String REGISTRY = "http://localhost:8081";

  private static KafkaProducer<Void, GenericArray<?>> createProducer() {
    Properties props = new Properties();
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
              VoidSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
              ConfluentSerializer.class);
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
              BOOSTRAP_SERVER);
    props.put(SCHEMA_REGISTRY_URL_CONFIG,
              REGISTRY);
    props.put(ProducerConfig.ACKS_CONFIG,
              "all");
    props.put(ProducerConfig.RETRIES_CONFIG,
              0);
    return new KafkaProducer<>(props);
  }

  private static KafkaConsumer<String, Json<?>> createConsumerWithJsonDeserializer() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
              BOOSTRAP_SERVER);
    props.put(ConsumerConfig.GROUP_ID_CONFIG,
              "group-json-deserializer-array-payments");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
              VoidDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
              ConfluentDeserializer.class);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
              "earliest");
    props.put(SCHEMA_REGISTRY_URL_CONFIG,
              REGISTRY);
    return new KafkaConsumer<>(props);
  }

  private static KafkaConsumer<Void, JsArray> createConsumerWithJsonArrDeserializer() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
              BOOSTRAP_SERVER);
    props.put(ConsumerConfig.GROUP_ID_CONFIG,
              "group-js-array-deserializer-array-payments");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
              VoidDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
              ConfluentArrayDeserializer.class);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
              "earliest");
    props.put(SCHEMA_REGISTRY_URL_CONFIG,
              REGISTRY);
    return new KafkaConsumer<>(props);
  }

  private static KafkaConsumer<Void, JsArray> createConsumerWithJsonArrSpecDeserializer() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
              BOOSTRAP_SERVER);
    props.put(ConsumerConfig.GROUP_ID_CONFIG,
              "group-js-array-spec-deserializer-array-payments");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
              VoidDeserializer.class);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
              "earliest");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
              ArrayPaymentDeserializer.class);
    props.put(SCHEMA_REGISTRY_URL_CONFIG,
              REGISTRY);
    return new KafkaConsumer<>(props);
  }

  static String TOPIC = "array_payments";


  @Test
  public void testCreateMessages() throws InterruptedException {
    Supplier<JsArray> gen = JsArrayGen.ofN(JsObjGen.of("id",
                                                       JsStrGen.alphabetic(),
                                                       "amount",
                                                       JsDoubleGen.arbitrary(100.0d,
                                                                             1000d)
                                                      ),
                                           10
                                          )
                                      .sample();
    int RECORDS = 10;
    try (producer) {
      for (long i = 0; i < RECORDS; i++) {
        final JsArray payments = gen.get();
        final ProducerRecord<Void, GenericArray<?>> record =
            new ProducerRecord<>(TOPIC,
                                 null,
                                 JsonToAvro.convert(payments,
                                                    Specs.arrayPaymentSpec));
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
    try (var consumer = createConsumerWithJsonArrDeserializer()) {
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

    try (var consumer = createConsumerWithJsonArrSpecDeserializer()) {
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
  }
}
