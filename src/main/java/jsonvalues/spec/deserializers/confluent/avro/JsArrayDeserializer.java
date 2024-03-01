package jsonvalues.spec.deserializers.confluent.avro;

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

public final class JsArrayDeserializer extends AbstractKafkaAvroDeserializer
    implements Deserializer<JsArray> {

  final boolean isJFREnabled;


  public JsArrayDeserializer() {
    isJFREnabled =
        Boolean.parseBoolean(System.getProperty("avro.spec.confluent.deserializer.jfr.enabled",
                                                "true"));
  }

  public JsArrayDeserializer(SchemaRegistryClient client) {
    this();
    this.schemaRegistry = client;
    this.ticker = ticker(client);
  }

  public JsArrayDeserializer(SchemaRegistryClient client,
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

  public JsArrayDeserializer(SchemaRegistryClient client,
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
      var event = new ConfluentAvroDeserializerEvent();
      event.begin();
      try {
        var container = deserializeToAvroContainer(topic,
                                                   headers,
                                                   bytes);
        event.result = ConfluentAvroDeserializerEvent.RESULT.SUCCESS.name();
        event.schema = container.getSchema()
                                .getFullName();
        return AvroToJson.toJsArray(container);
      } catch (Exception e) {
        event.result = ConfluentAvroDeserializerEvent.RESULT.FAILURE.name();
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
      return deserialize(Objects.requireNonNull(topic),
                         Objects.requireNonNull(headers),
                         Objects.requireNonNull(bytes));
    }

  }

  private GenericArray<?> deserializeToAvroContainer(final String topic,
                                                     final Headers headers,
                                                     final byte[] bytes) {
    return (GenericArray<?>) deserialize(Objects.requireNonNull(topic),
                                         isKey,
                                         headers,
                                         Objects.requireNonNull(bytes),
                                         specificAvroReaderSchema);
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
