package jsonvalues.spec.deserializers.confluent;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import jsonvalues.Json;
import jsonvalues.spec.AvroSpecFun;
import jsonvalues.spec.AvroToJson;
import jsonvalues.spec.deserializers.confluent.ConfluentDeserializerEvent.RESULT;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Deserializer for deserializing Kafka messages into JSON (JsObj or JsArray). This class extends the
 * {@link AbstractKafkaAvroDeserializer} class from the Confluent Kafka Avro library. It implements the
 * {@link Deserializer} interface for deserializing Kafka messages into JSON. It's integrated with the Java Flight
 * Recorder (JFR) for debugging and monitoring Avro deserialization events (use the property
 * "avro.spec.confluent.deserializer.jfr.enabled" to enable or disable JFR integration). It's also integrated with the
 * Confluent Schema Registry for deserializing Avro messages.
 */
public final class ConfluentDeserializer extends AbstractKafkaAvroDeserializer
    implements Deserializer<Json<?>> {

  final boolean isJFREnabled;


  public ConfluentDeserializer() {
    isJFREnabled =
        Boolean.parseBoolean(System.getProperty("avro.spec.confluent.deserializer.jfr.enabled",
                                                "true"));
  }

  public ConfluentDeserializer(SchemaRegistryClient client) {
    this();
    this.schemaRegistry = client;
    this.ticker = ticker(client);
  }

  public ConfluentDeserializer(final SchemaRegistryClient client,
                               final Map<String, ?> props) {
    this(Objects.requireNonNull(client),
         Objects.requireNonNull(props),
         false);
  }

  @Override
  public void configure(final Map<String, ?> props,
                        final boolean isKey) {
    this.isKey = isKey;
    configure(deserializerConfig(Objects.requireNonNull(props)),
              null);
  }

  public ConfluentDeserializer(final SchemaRegistryClient client,
                               final Map<String, ?> props,
                               final boolean isKey) {
    this();
    this.schemaRegistry = Objects.requireNonNull(client);
    this.ticker = ticker(Objects.requireNonNull(client));
    this.isKey = isKey;
    configure(deserializerConfig(props));
  }

  @Override
  public Json<?> deserialize(final String topic,
                             final byte[] bytes) {

    return deserialize(Objects.requireNonNull(topic),
                       null,
                       bytes);

  }

  @Override
  public Json<?> deserialize(final String topic,
                             final Headers headers,
                             final byte[] bytes) {
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
        event.result = RESULT.SUCCESS.name();

        return switch (container) {
          case GenericArray<?> array -> AvroToJson.convert(array);
          case GenericRecord record -> AvroToJson.convert(record);
          default -> throw new IllegalStateException(
              "Only GenericArray and GenericRecord are supported. Received type: " + container.getClass());
        };

      } catch (Exception e) {
        event.result = RESULT.FAILURE.name();
        event.exception = AvroSpecFun.findUltimateCause(e).toString();
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
      return deserialize(topic,
                         headers,
                         bytes);
    }

  }

  private GenericContainer deserializeToAvroContainer(final String topic,
                                                      final Headers headers,
                                                      final byte[] bytes) {
    return (GenericContainer) deserialize(Objects.requireNonNull(topic),
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
