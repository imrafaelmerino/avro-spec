package jsonvalues.avro;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;
import jio.test.junit.Debugger;
import jsonvalues.JsObj;
import jsonvalues.Json;
import jsonvalues.gen.JsDoubleGen;
import jsonvalues.gen.JsObjGen;
import jsonvalues.gen.JsStrGen;
import jsonvalues.spec.JsonToAvro;
import jsonvalues.spec.deserializers.avro.JsonSpecDeserializer;
import jsonvalues.spec.deserializers.avro.JsonSpecDeserializerBuilder;
import jsonvalues.spec.serializers.avro.JsonSpecSerializer;
import jsonvalues.spec.serializers.avro.JsonSpecSerializerBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
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
  static Debugger debugger = MyDebuggers.avroDebugger(Duration.ofSeconds(30));


  final static KafkaProducer<String, byte[]> producer = createProducer();


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

  JsonSpecSerializer specSerializer =
      JsonSpecSerializerBuilder.of(Specs.paymentSpec)
                              .build();

  JsonSpecDeserializer specDeserializer =
      JsonSpecDeserializerBuilder.of(Specs.paymentSpec)
                                .build();

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

        final ProducerRecord<String, byte[]> record =
            new ProducerRecord<>(TOPIC,
                                 payment.getStr("id") + i,
                                 specSerializer.binaryEncode(payment));
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
          System.out.printf("offset = %d, key = %s, value = %s%n",
                            record.offset(),
                            record.key(),
                            specDeserializer.binaryDecode(record.value().get())
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
