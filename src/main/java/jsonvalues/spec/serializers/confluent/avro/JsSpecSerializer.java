package jsonvalues.spec.serializers.confluent.avro;


import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import jsonvalues.Json;
import jsonvalues.spec.AvroSpecFun;
import jsonvalues.spec.JsSpec;
import jsonvalues.spec.JsonToAvro;
import jsonvalues.spec.SpecToAvroSchema;
import org.apache.avro.generic.GenericContainer;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

// SuppressWarnings: these constructors are from confluent source code. Anyway, this-scape warning doesn't seem to be an issue to be concerned.
@SuppressWarnings("this-escape")
public abstract class JsSpecSerializer extends AbstractKafkaAvroSerializer implements Serializer<Json<?>> {

  protected abstract boolean isJFREnabled();

  protected abstract JsSpec getSpec();

  final AvroSchema schema;

  public JsSpecSerializer() {
    schema = new AvroSchema(SpecToAvroSchema.convert(getSpec()));
  }


  public JsSpecSerializer(SchemaRegistryClient client) {
    this();
    this.schemaRegistry = client;
    this.ticker = ticker(client);
  }

  public JsSpecSerializer(SchemaRegistryClient client,
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
                          Json<?> data) {
    return this.serialize(Objects.requireNonNull(topic),
                          null,
                          Objects.requireNonNull(data));
  }

  @Override
  public byte[] serialize(String topic,
                          Headers headers,
                          Json<?> json) {
    if (json == null) {
      return null;
    }
    if (isJFREnabled()) {
      var event = new ConfluentAvroSerializerEvent();
      event.begin();
      try {
        var result = serializeContainer(Objects.requireNonNull(topic),
                                        headers,
                                        JsonToAvro.convert(json,
                                                           getSpec())
                                       );
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
          event.commit();
        }
      }
    } else {
      return serializeContainer(topic,
                                headers,
                                JsonToAvro.convert(json,
                                                   getSpec())
                               );
    }
  }

  private byte[] serializeContainer(final String topic,
                                    final Headers headers,
                                    final GenericContainer record) {
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
