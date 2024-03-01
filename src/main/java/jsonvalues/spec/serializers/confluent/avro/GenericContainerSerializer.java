package jsonvalues.spec.serializers.confluent.avro;


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

public final class GenericContainerSerializer
    extends AbstractKafkaAvroSerializer
    implements Serializer<GenericContainer> {

  final boolean isJFREnabled;

  public GenericContainerSerializer() {
    isJFREnabled =
        Boolean.parseBoolean(System.getProperty("avro.spec.confluent.serializer.jfr.enabled",
                                                "true"));
  }

  public GenericContainerSerializer(SchemaRegistryClient client) {
    this();
    this.schemaRegistry = client;
    this.ticker = ticker(client);
  }

  public GenericContainerSerializer(SchemaRegistryClient client,
                                    Map<String, ?> props) {
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
  public byte[] serialize(String topic,
                          GenericContainer data) {
    return this.serialize(Objects.requireNonNull(topic),
                          null,
                          Objects.requireNonNull(data));
  }

  @Override
  public byte[] serialize(String topic,
                          Headers headers,
                          GenericContainer container) {
    if (container == null) {
      return null;
    }
    if (isJFREnabled) {
      var event = new ConfluentAvroSerializerEvent();
      event.begin();
      try {
        var result = serializeContainer(Objects.requireNonNull(topic),
                                        headers,
                                        container);
        event.result = ConfluentAvroSerializerEvent.RESULT.SUCCESS.name();
        event.bytes = result.length;
        return result;
      } catch (Exception e) {
        event.result = ConfluentAvroSerializerEvent.RESULT.FAILURE.name();
        event.exception = AvroSpecFun.findUltimateCause(e)
                                     .toString();
        throw e;
      } finally {
        event.end();
        if (event.shouldCommit()) {

          event.topic = topic;
          event.schema = container.getSchema()
                                  .getFullName();
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
