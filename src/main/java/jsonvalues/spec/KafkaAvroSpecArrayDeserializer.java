package jsonvalues.spec;

import java.util.Objects;
import jsonvalues.JsArray;
import org.apache.kafka.common.serialization.Deserializer;

public abstract class KafkaAvroSpecArrayDeserializer implements Deserializer<JsArray> {

  final ArraySpecDeserializer specDeserializer;
  final boolean enableJFR;

  public KafkaAvroSpecArrayDeserializer(final ArraySpecDeserializerBuilder builder,
                                        final boolean enableJFR) {

    this.specDeserializer = Objects.requireNonNull(builder)
                                   .build();
    this.enableJFR = enableJFR;
  }


  @Override
  public JsArray deserialize(final String topic,
                             final byte[] data) {
    if (enableJFR) {

      var event = new KafkaDeserializerEvent();
      event.begin();
      try {
        var array = specDeserializer.binaryDecode(data);
        assert specDeserializer.readerSpec.test(array)
                                          .isEmpty() :
            "The json array doesn't conform the spec. Errors: %s".formatted(specDeserializer.readerSpec.test(array));

        event.result = KafkaDeserializerEvent.RESULT.SUCCESS.name();
        return array;
      } catch (Exception e) {
        event.result = KafkaDeserializerEvent.RESULT.FAILURE.name();
        event.exception = DebuggerUtils.findUltimateCause(e)
                                       .toString();
        throw e;
      } finally {
        event.end();
        if (event.shouldCommit()) {
          event.topic = topic;
          event.schema = specDeserializer.readerSchema.getElementType()
                                                      .getFullName();
          event.counter = Counters.deserializerCounter.incrementAndGet();
          event.commit();
        }
      }
    } else {
      return specDeserializer.binaryDecode(data);
    }
  }
}
