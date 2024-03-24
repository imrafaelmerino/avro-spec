package jsonvalues.spec.serializers.confluent;


import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import jsonvalues.spec.AvroSpecFun;
import org.apache.avro.generic.GenericContainer;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Serializer for generic Avro containers used with Kafka and integration with Confluent's Kafka Schema Registry. This
 * serializer extends {@link io.confluent.kafka.serializers.AbstractKafkaAvroSerializer} and implements
 * {@link org.apache.kafka.common.serialization.Serializer}. It provides serialization functionality for
 * {@link org.apache.avro.generic.GenericContainer} instances.
 * <p>
 * Integration with Confluent's Kafka Schema Registry allows for seamless serialization and deserialization of Avro
 * records while ensuring compatibility with evolving schemas.
 * <p>
 * JFR (Java Flight Recorder) integration is enabled by default. Use the system property
 * {@code avro.spec.confluent.serializer.jfr.enabled} to disable JFR integration if needed.
 */
public final class ConfluentSerializer
    extends AbstractKafkaAvroSerializer implements
                                        Serializer<GenericContainer> {

  final boolean isJFREnabled;

  /**
   * Constructs a new {@code GenericContainerSerializer}.
   */
  public ConfluentSerializer() {
    isJFREnabled =
        Boolean.parseBoolean(System.getProperty("avro.spec.confluent.serializer.jfr.enabled",
                                                "true")
                            );
  }

  /**
   * Constructs a new {@code GenericContainerSerializer} with a specified schema registry client.
   *
   * @param client The schema registry client to use.
   */
  public ConfluentSerializer(final SchemaRegistryClient client) {
    this();
    this.schemaRegistry = client;
    this.ticker = ticker(client);
  }

  /**
   * Constructs a new {@code GenericContainerSerializer} with a specified schema registry client and properties.
   *
   * @param client The schema registry client to use.
   * @param props  The properties for the serializer.
   */
  public ConfluentSerializer(final SchemaRegistryClient client,
                             final Map<String, ?> props) {
    this();
    this.schemaRegistry = client;
    this.ticker = ticker(client);
    configure(serializerConfig(props));
  }

  @Override
  public void configure(Map<String, ?> configs,
                        boolean isKey) {
    this.isKey = isKey;
    configure(new KafkaAvroSerializerConfig(configs));
  }

  @Override
  public byte[] serialize(final String topic,
                          final GenericContainer data) {
    return this.serialize(Objects.requireNonNull(topic),
                          null,
                          Objects.requireNonNull(data));
  }

  @Override
  public byte[] serialize(final String topic,
                          final Headers headers,
                          final GenericContainer container) {
    if (container == null) {
      return null;
    }
    if (isJFREnabled) {
      var event = new ConfluentSerializerEvent();
      event.begin();
      try {
        var result = serializeContainer(Objects.requireNonNull(topic),
                                        headers,
                                        container);
        event.result = ConfluentSerializerEvent.RESULT.SUCCESS.name();
        event.bytes = result.length;
        return result;
      } catch (Exception e) {
        event.result = ConfluentSerializerEvent.RESULT.FAILURE.name();
        event.exception = AvroSpecFun.findUltimateCause(e)
                                     .toString();
        throw e;
      } finally {
        event.end();
        if (event.shouldCommit()) {
          event.topic = topic;
          event.commit();
        }
      }

    } else {
      return serializeContainer(topic,
                                headers,
                                container);
    }
  }

  private byte[] serializeContainer(final String topic,
                                    final Headers headers,
                                    final GenericContainer record) {
    AvroSchema schema = new AvroSchema(record.getSchema());
    return serializeImpl(getSubjectName(topic,
                                        isKey,
                                        record,
                                        schema),
                         topic,
                         headers,
                         record,
                         schema);
  }

  @Override
  public void close() {
    try {
      super.close();
    } catch (IOException e) {
      throw new RuntimeException("Exception while closing serializer",
                                 e);
    }
  }


}
