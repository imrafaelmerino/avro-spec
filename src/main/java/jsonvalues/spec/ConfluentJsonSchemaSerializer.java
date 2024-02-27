package jsonvalues.spec;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.serializers.json.AbstractKafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import jsonvalues.Json;
import jsonvalues.spec.ConfluentSerializerEvent.RESULT;
import org.apache.avro.generic.GenericContainer;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

public abstract class ConfluentJsonSchemaSerializer extends
                                                    AbstractKafkaJsonSchemaSerializer<GenericContainer> implements
                                                                                                        Serializer<Json<?>> {

  protected abstract JsSpec getSpec();

  private final JsonSchema schema;

  protected abstract boolean isJFREnabled();

  public ConfluentJsonSchemaSerializer() {
    schema = new JsonSchema(SpecToSchema.convert(Objects.requireNonNull(getSpec()))
                                        .toString());
  }

  public ConfluentJsonSchemaSerializer(SchemaRegistryClient client) {
    this();
    this.schemaRegistry = Objects.requireNonNull(client);
    this.ticker = this.ticker(client);
  }


  public ConfluentJsonSchemaSerializer(SchemaRegistryClient client,
                                       Map<String, ?> props) {
    this();
    this.schemaRegistry = Objects.requireNonNull(client);
    this.ticker = this.ticker(client);
    this.configure(this.serializerConfig(Objects.requireNonNull(props)));
  }

  public void configure(Map<String, ?> config,
                        boolean isKey) {
    this.isKey = isKey;
    this.configure(new KafkaJsonSchemaSerializerConfig(Objects.requireNonNull(config)));
  }

  @Override
  public byte[] serialize(String topic,
                          Json<?> json) {
    return this.serialize(topic,
                          null,
                          json);
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
        event.result = RESULT.SUCCESS.name();
        return result;
      } catch (Exception e) {
        event.result = RESULT.FAILURE.name();
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
    var record = JsonToAvro.toAvro(json,
                                   getSpec());

    return this.serializeImpl(this.getSubjectName(topic,
                                                  this.isKey,
                                                  record,
                                                  schema),
                              topic,
                              headers,
                              record,
                              schema);
  }


  public void close() {
    try {
      super.close();
    } catch (IOException var2) {
      throw new RuntimeException("Exception while closing serializer",
                                 var2);
    }
  }
}
