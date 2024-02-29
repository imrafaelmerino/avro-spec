package jsonvalues.spec;

import java.util.Objects;
import jsonvalues.JsObj;
import org.apache.kafka.common.serialization.Deserializer;

public abstract class KafkaAvroSpecObjDeserializer implements Deserializer<JsObj> {

  final ObjSpecDeserializer specDeserializer;
  final boolean enableJFR;

  public KafkaAvroSpecObjDeserializer(final ObjSpecDeserializerBuilder builder,
                                      final boolean enableJFR
                                     ) {

    this.specDeserializer = Objects.requireNonNull(builder)
                                   .build();
    this.enableJFR = enableJFR;
  }


  @Override
  public JsObj deserialize(final String topic,
                           final byte[] data) {
    if (enableJFR) {

      var event = new KafkaDeserializerEvent();
      event.begin();
      try {
        var json = specDeserializer.binaryDecode(data);
        assert specDeserializer.readerSpec.test(json)
                                          .isEmpty() :
            "The json object doesn't conform the spec. Errors: %s".formatted(specDeserializer.readerSpec.test(json));

        event.result = KafkaDeserializerEvent.RESULT.SUCCESS.name();
        return json;
      } catch (Exception e) {
        event.result = KafkaDeserializerEvent.RESULT.FAILURE.name();
        event.exception = DebuggerUtils.findUltimateCause(e)
                                       .toString();
        throw e;
      } finally {
        event.end();
        if (event.shouldCommit()) {
          event.topic = topic;
          event.schema = specDeserializer.readerSchema.getFullName();
          event.counter = Counters.deserializerCounter.incrementAndGet();
          event.commit();
        }
      }
    } else {
      return specDeserializer.binaryDecode(data);
    }
  }
}
