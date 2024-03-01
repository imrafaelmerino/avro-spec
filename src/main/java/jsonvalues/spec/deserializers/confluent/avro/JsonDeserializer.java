package jsonvalues.spec.deserializers.confluent.avro;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import jsonvalues.Json;
import jsonvalues.spec.AvroToJson;
import jsonvalues.spec.deserializers.confluent.avro.ConfluentAvroDeserializerEvent.RESULT;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

public final class JsonDeserializer extends AbstractKafkaAvroDeserializer
    implements Deserializer<Json<?>> {


  final boolean isJFREnabled;


  public JsonDeserializer() {
    isJFREnabled =
        Boolean.parseBoolean(System.getProperty("avro.spec.confluent.deserializer.jfr.enabled",
                                                "true"));
  }

  public JsonDeserializer(SchemaRegistryClient client) {
    this();
    this.schemaRegistry = client;
    this.ticker = ticker(client);
  }

  public JsonDeserializer(SchemaRegistryClient client,
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

  public JsonDeserializer(SchemaRegistryClient client,
                          Map<String, ?> props,
                          boolean isKey) {
    this();
    this.schemaRegistry = Objects.requireNonNull(client);
    this.ticker = ticker(Objects.requireNonNull(client));
    this.isKey = isKey;
    configure(deserializerConfig(props));
  }

  @Override
  public Json<?> deserialize(String topic,
                             byte[] bytes) {

    return deserialize(Objects.requireNonNull(topic),
                       null,
                       bytes);

  }

  @Override
  public Json<?> deserialize(String topic,
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
        event.result = RESULT.SUCCESS.name();
        event.schema = container.getSchema()
                                .getFullName();
        return switch (container) {
          case GenericArray<?> array -> AvroToJson.toJsArray(array);
          case GenericRecord record -> AvroToJson.toJsObj(record);
          default -> throw new IllegalStateException(
              "Only GenericContainer and GenericRecord are supported. Received type: " + container.getClass());
        };

      } catch (Exception e) {
        event.result = RESULT.FAILURE.name();
        event.exception = findUltimateCause(e).toString();
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

  private GenericContainer deserializeToAvroContainer(final String topic,
                                                      final Headers headers,
                                                      final byte[] bytes) {
    return (GenericContainer) deserialize(Objects.requireNonNull(topic),
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

  /**
   * Finds the ultimate cause in the exception chain or the exception if the cause is null.
   *
   * @param exception The initial exception to start the search from.
   * @return The ultimate cause in the exception chain.
   * @throws NullPointerException If the provided exception is {@code null}.
   */
  private Throwable findUltimateCause(Throwable exception) {
    var ultimateCause = Objects.requireNonNull(exception);

    while (ultimateCause.getCause() != null) {
      ultimateCause = ultimateCause.getCause();
    }

    return ultimateCause;
  }
}
