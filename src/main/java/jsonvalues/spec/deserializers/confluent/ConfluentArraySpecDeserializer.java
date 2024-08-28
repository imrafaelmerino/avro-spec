package jsonvalues.spec.deserializers.confluent;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import jsonvalues.JsArray;
import jsonvalues.spec.AvroSpecFun;
import jsonvalues.spec.AvroToJson;
import jsonvalues.spec.JsSpec;
import jsonvalues.spec.SpecToAvroSchema;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

// SuppressWarnings: these constructors are from confluent source code. Anyway, this-scape warning doesn't seem to be an issue to be concerned.
@SuppressWarnings("this-escape")
public abstract class ConfluentArraySpecDeserializer extends AbstractKafkaAvroDeserializer
    implements Deserializer<JsArray> {

  /**
   * Get the JSON specification defining the structure and constraints of the JSON object.
   *
   * @return The JSON specification
   */
  protected abstract JsSpec getSpec();

  /**
   * Determine if Java Flight Recorder (JFR) is enabled for event tracking during deserialization.
   *
   * @return true if JFR is enabled, false otherwise
   */
  protected abstract boolean isJFREnabled();

  private final Schema schema;


  public ConfluentArraySpecDeserializer() {
    this.schema = SpecToAvroSchema.convert(getSpec());
  }

  @SuppressWarnings("this-escape")
  public ConfluentArraySpecDeserializer(SchemaRegistryClient client) {
    this();
    this.schemaRegistry = client;
    this.ticker = ticker(client);
  }

  public ConfluentArraySpecDeserializer(final SchemaRegistryClient client,
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

  @SuppressWarnings("this-escape")
  public ConfluentArraySpecDeserializer(final SchemaRegistryClient client,
                                        final Map<String, ?> props,
                                        final boolean isKey) {
    this();
    this.schemaRegistry = Objects.requireNonNull(client);
    this.ticker = ticker(Objects.requireNonNull(client));
    this.isKey = isKey;
    configure(deserializerConfig(props));
  }

  @Override
  public JsArray deserialize(final String topic,
                             final byte[] bytes) {

    return deserialize(Objects.requireNonNull(topic),
                       null,
                       bytes);

  }

  @Override
  public JsArray deserialize(final String topic,
                             final Headers headers,
                             final byte[] bytes) {
    if (bytes == null) {
      return null;
    }

    if (isJFREnabled()) {
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

  private GenericArray<?> deserializeToAvroContainer(String topic,
                                                     Headers headers,
                                                     byte[] bytes) {
    return (GenericArray<?>) deserialize(Objects.requireNonNull(topic),
                                         isKey,
                                         headers,
                                         Objects.requireNonNull(bytes),
                                         schema);
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
