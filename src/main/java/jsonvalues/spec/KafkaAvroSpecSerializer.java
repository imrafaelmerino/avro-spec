package jsonvalues.spec;

import java.util.Objects;
import jsonvalues.Json;
import jsonvalues.spec.ConfluentSerializerEvent.RESULT;
import org.apache.kafka.common.serialization.Serializer;

public abstract class KafkaAvroSpecSerializer implements Serializer<Json<?>> {

  final SpecSerializer specSerializer;
  private final boolean enableJFR;


  public KafkaAvroSpecSerializer(final SpecSerializerBuilder builder,
                                 final boolean enableJFR) {

    this.specSerializer = Objects.requireNonNull(builder)
                                 .build();
    this.enableJFR = enableJFR;
  }

  @Override
  public byte[] serialize(final String topic,
                          final Json<?> json) {
    if (enableJFR) {

      var event = new KafkaSerializerEvent();
      event.begin();
      try {
        var result = specSerializer.binaryEncode(json);
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
          event.schema = specSerializer.schema.getFullName();
          event.counter = Counters.serializerCounter.incrementAndGet();
          event.commit();
        }
      }
    } else {
      return specSerializer.binaryEncode(json);
    }
  }
}
