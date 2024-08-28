package jsonvalues.spec.deserializers.confluent;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import jsonvalues.JsArray;
import jsonvalues.spec.AvroSpecFun;
import jsonvalues.spec.AvroToJson;
import org.apache.avro.generic.GenericArray;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Deserializer for deserializing Kafka messages into JSON array (JsArray). This class extends the
 * {@link AbstractKafkaAvroDeserializer} class from the Confluent Kafka Avro library. It implements the
 * {@link Deserializer} interface for deserializing Kafka messages into JSON arrays (JsArray). It's integrated with the
 * Java Flight Recorder (JFR) for debugging and monitoring Avro deserialization events (use the property
 * "avro.spec.confluent.deserializer.jfr.enabled" to enable or disable JFR integration). It's also integrated with the
 * Confluent Schema Registry for deserializing Avro messages.
 */
public final class ConfluentArrayDeserializer extends AbstractKafkaAvroDeserializer
    implements Deserializer<JsArray> {

  final boolean isJFREnabled;


  public ConfluentArrayDeserializer() {
    isJFREnabled =
        Boolean.parseBoolean(System.getProperty("avro.spec.confluent.deserializer.jfr.enabled",
                                                "true"));
  }

  public ConfluentArrayDeserializer(SchemaRegistryClient client) {
    this();
    this.schemaRegistry = client;
    this.ticker = ticker(client);
  }

  public ConfluentArrayDeserializer(SchemaRegistryClient client,
                                    Map<String, ?> props) {
    this(Objects.requireNonNull(client),
         Objects.requireNonNull(props),
         false);
  }

  @Override
  public void configure(Map<String, ?> props,
                        boolean isKey) {
    this.isKey = isKey;
    configure(deserializerConfig(Objects.requireNonNull(props)),
              null);
  }

  public ConfluentArrayDeserializer(SchemaRegistryClient client,
                                    Map<String, ?> props,
                                    boolean isKey) {
    this();
    this.schemaRegistry = Objects.requireNonNull(client);
    this.ticker = ticker(Objects.requireNonNull(client));
    this.isKey = isKey;
    configure(deserializerConfig(props));
  }

  @Override
  public JsArray deserialize(String topic,
                             byte[] bytes) {

    return deserialize(Objects.requireNonNull(topic),
                       null,
                       bytes);

  }

  @Override
  public JsArray deserialize(String topic,
                             Headers headers,
                             byte[] bytes) {
    if (bytes == null) {
      return null;
    }

    if (isJFREnabled) {
      var event = new ConfluentDeserializerEvent();
      event.begin();
      try {
        var container = deserializeToAvroContainer(topic,
                                                   headers,
                                                   bytes);
        event.result = ConfluentDeserializerEvent.RESULT.SUCCESS.name();
        return AvroToJson.convert(container);
      } catch (Exception e) {
        event.result = ConfluentDeserializerEvent.RESULT.FAILURE.name();
        event.exception = AvroSpecFun.findUltimateCause(e)
                                     .toString();
        throw e;
      } finally {
        event.end();
        if (event.shouldCommit()) {
          event.bytes = bytes.length;
          event.topic = topic;
          event.commit();
        }
      }

    } else {
      var container = deserializeToAvroContainer(topic,
                                                 headers,
                                                 bytes);
      return AvroToJson.convert(container);
    }

  }

  private GenericArray<?> deserializeToAvroContainer(final String topic,
                                                     final Headers headers,
                                                     final byte[] bytes) {
    return (GenericArray<?>) deserialize(Objects.requireNonNull(topic),
                                         isKey,
                                         headers,
                                         Objects.requireNonNull(bytes),
                                         null);
  }


  @Override
  public void close() {
    try {
      super.close();
    } catch (IOException e) {
      throw new RuntimeException("Exception while closing deserializer",
                                 e);
    }
  }

}
