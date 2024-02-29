package jsonvalues.spec;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import jsonvalues.JsArray;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

public abstract class ConfluentAvroArrDeserializer extends AbstractKafkaAvroDeserializer implements
                                                                                         Deserializer<JsArray> {

  protected abstract JsArraySpec getSpec();

  private final Schema schema;

  protected abstract boolean isJFREnabled();


  /**
   * Constructor used by Kafka consumer.
   */
  public ConfluentAvroArrDeserializer() {
    schema = SpecToAvroSchema.convert(Objects.requireNonNull(getSpec()));
  }

  public ConfluentAvroArrDeserializer(SchemaRegistryClient client) {
    this();
    this.schemaRegistry = Objects.requireNonNull(client);
    this.ticker = ticker(client);
  }

  public ConfluentAvroArrDeserializer(SchemaRegistryClient client,
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

  public ConfluentAvroArrDeserializer(SchemaRegistryClient client,
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
                       Objects.requireNonNull(bytes));
  }

  @Override
  public JsArray deserialize(String topic,
                             Headers headers,
                             byte[] bytes) {
    if (isJFREnabled()) {
      var event = new ConfluentDeserializerEvent();
      event.begin();
      try {
        var array = deserializeToArray(topic,
                                       headers,
                                       bytes);
        assert getSpec().test(array)
                        .isEmpty() :
            "The json array doesn't conform the spec. Errors: %s".formatted(getSpec().test(array));
        event.result = ConfluentDeserializerEvent.RESULT.SUCCESS.name();
        return array;
      } catch (Exception e) {
        event.result = ConfluentDeserializerEvent.RESULT.FAILURE.name();
        event.exception = DebuggerUtils.findUltimateCause(e)
                                       .toString();
        throw e;
      } finally {
        event.end();
        if (event.shouldCommit()) {
          event.topic = topic;
          event.schema = schema.getElementType()
                               .getFullName();
          event.counter = Counters.deserializerCounter.incrementAndGet();
          event.commit();
        }
      }

    } else {
      return deserializeToArray(topic,
                                headers,
                                bytes);
    }

  }

  private JsArray deserializeToArray(final String topic,
                                     final Headers headers,
                                     final byte[] bytes) {
    GenericArray<?> array =
        (GenericArray<?>) deserialize(Objects.requireNonNull(topic),
                                      isKey,
                                      headers,
                                      Objects.requireNonNull(bytes),
                                      schema);

    return AvroToJson.toJsArray(array);
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
