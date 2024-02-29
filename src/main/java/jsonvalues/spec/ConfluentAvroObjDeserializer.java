package jsonvalues.spec;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import jsonvalues.JsObj;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

public abstract class ConfluentAvroObjDeserializer extends AbstractKafkaAvroDeserializer
    implements Deserializer<JsObj> {

  protected abstract JsObjSpec getSpec();

  protected abstract boolean isJFREnabled();

  private final Schema schema;


  public ConfluentAvroObjDeserializer() {
    schema = SpecToAvroSchema.convert(Objects.requireNonNull(getSpec()));
  }

  public ConfluentAvroObjDeserializer(SchemaRegistryClient client) {
    this();
    this.schemaRegistry = client;
    this.ticker = ticker(client);
  }

  public ConfluentAvroObjDeserializer(SchemaRegistryClient client,
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

  public ConfluentAvroObjDeserializer(SchemaRegistryClient client,
                                      Map<String, ?> props,
                                      boolean isKey) {
    this();
    this.schemaRegistry = Objects.requireNonNull(client);
    this.ticker = ticker(Objects.requireNonNull(client));
    this.isKey = isKey;
    configure(deserializerConfig(props));
  }

  @Override
  public JsObj deserialize(String topic,
                           byte[] bytes) {

    return deserialize(Objects.requireNonNull(topic),
                       null,
                       Objects.requireNonNull(bytes));

  }

  @Override
  public JsObj deserialize(String topic,
                           Headers headers,
                           byte[] bytes) {

    if (isJFREnabled()) {
      var event = new ConfluentDeserializerEvent();
      event.begin();
      try {
        var json = deserializeToObj(topic,
                                      headers,
                                      bytes);
        event.result = ConfluentDeserializerEvent.RESULT.SUCCESS.name();
        assert getSpec().test(json)
                        .isEmpty() :
            "The json object doesn't conform the spec. Errors: %s".formatted(getSpec().test(json));
        return json;
      } catch (Exception e) {
        event.result = ConfluentDeserializerEvent.RESULT.FAILURE.name();
        event.exception = DebuggerUtils.findUltimateCause(e)
                                       .toString();
        throw e;
      } finally {
        event.end();
        if (event.shouldCommit()) {
          event.topic = topic;
          event.schema = schema.getFullName();
          event.counter = Counters.deserializerCounter.incrementAndGet();
          event.commit();
        }
      }

    } else {
      return deserialize(Objects.requireNonNull(topic),
                         Objects.requireNonNull(headers),
                         Objects.requireNonNull(bytes));
    }

  }

  private JsObj deserializeToObj(final String topic,
                                 final Headers headers,
                                 final byte[] bytes) {
    GenericRecord record = (GenericRecord) deserialize(Objects.requireNonNull(topic),
                                                       isKey,
                                                       headers,
                                                       Objects.requireNonNull(bytes),
                                                       schema);
    return AvroToJson.toJsObj(record);
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
