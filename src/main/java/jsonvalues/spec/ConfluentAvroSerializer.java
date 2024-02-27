package jsonvalues.spec;


import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import jsonvalues.Json;
import jsonvalues.spec.ConfluentSerializerEvent.RESULT;
import org.apache.avro.generic.GenericContainer;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

public abstract class ConfluentAvroSerializer
    extends AbstractKafkaAvroSerializer
    implements Serializer<Json<?>> {

  protected abstract JsSpec getSpec();


  protected abstract boolean isJFREnabled();

  private final AvroSchema schema;

  /**
   * Constructor used by Kafka producer.
   */
  public ConfluentAvroSerializer() {
    schema = new AvroSchema(SpecToSchema.convert(Objects.requireNonNull(getSpec())));
  }

  public ConfluentAvroSerializer(SchemaRegistryClient client) {
    this();
    this.schemaRegistry = Objects.requireNonNull(client);
    this.ticker = ticker(client);
  }

  public ConfluentAvroSerializer(SchemaRegistryClient client,
                                 Map<String, ?> props) {
    this();
    this.schemaRegistry = Objects.requireNonNull(client);
    this.ticker = ticker(client);
    configure(serializerConfig(Objects.requireNonNull(props)));
  }

  @Override
  public void configure(Map<String, ?> configs,
                        boolean isKey) {
    this.isKey = isKey;
    configure(new KafkaAvroSerializerConfig(Objects.requireNonNull(configs)));
  }

  @Override
  public byte[] serialize(String topic,
                          Json<?> data) {
    return serialize(topic,
                     null,
                     data);
  }

  @Override
  public byte[] serialize(String topic,
                          Headers headers,
                          Json<?> json) {
    if (isJFREnabled()) {
      var event = new ConfluentSerializerEvent();
      event.begin();
      try {
        var result = serializeJson(topic,
                                   headers,
                                   json);
        event.result = ConfluentSerializerEvent.RESULT.SUCCESS.name();
        return result;
      } catch (Exception e) {
        event.result = ConfluentSerializerEvent.RESULT.FAILURE.name();
        event.exception = DebuggerUtils.findUltimateCause(e)
                                       .toString();
        throw e;
      } finally {
        event.end();
        if (event.shouldCommit()) {
          event.topic = topic;
          event.schema = schema.name();
          event.counter = Counters.serializerCounter.incrementAndGet();
          event.commit();
        }
      }

    } else {
      return serializeJson(topic,
                           headers,
                           json);
    }

  }

  private byte[] serializeJson(final String topic,
                               final Headers headers,
                               final Json<?> json) {
    JsSpec spec = getSpec();

    GenericContainer record = JsonToAvro.toAvro(Objects.requireNonNull(json),
                                                spec);

    return serializeImpl(getSubjectName(topic,
                                        isKey,
                                        record,
                                        schema),
                         topic,
                         headers,
                         record,
                         schema
                        );
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
