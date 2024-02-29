package jsonvalues.avro;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.util.Properties;
import java.util.Random;
import java.util.function.Supplier;
import jsonvalues.JsObj;
import jsonvalues.avro.serializers.PeripheralAvroConfluentSerializer;
import jsonvalues.avro.serializers.PeripheralJsonSchemaConfluentSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

/**
 * The following schema has been created in the topic "payments" in the kafka cluster.
 * <p>
 * { "namespace": "io.confluent.examples.clients.basicavro", "type": "record", "name": "Payment", "fields": [ {"name":
 * "id", "type": "string"}, {"name": "amount", "type": "double"} ] }
 */
public class PeripheralTopicJsonSchemaProducerTest {


  final static KafkaProducer<Void, JsObj> producer = createProducer();

  private static KafkaProducer<Void, JsObj> createProducer() {
    Properties props = new Properties();
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
              StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
              PeripheralJsonSchemaConfluentSerializer.class);
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
              "localhost:29092");
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
              "http://localhost:8081");
    props.put(ProducerConfig.ACKS_CONFIG,
              "all");
    props.put(ProducerConfig.RETRIES_CONFIG,
              0);
    return new KafkaProducer<>(props);
  }

  static String TOPIC = "peripheral";

  @Test
  public void testCreateMessages() {
    Supplier<JsObj> gen = Gens.peripheralGen.sample(new Random());

    try (producer) {

      for (long i = 0; i < 10; i++) {
        JsObj peripheral = gen.get();
        ProducerRecord<Void, JsObj> record =
            new ProducerRecord<>(TOPIC,
                                 null,
                                 peripheral);
        producer.send(record);
        Thread.sleep(1000L);
      }

      producer.flush();
      System.out.printf("Successfully produced 10 messages to a topic called %s%n",
                        TOPIC);

    } catch (final SerializationException | InterruptedException e) {
      e.printStackTrace();
    }
  }

}
