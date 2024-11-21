package jsonvalues.spec.serializers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import jsonvalues.Json;
import jsonvalues.spec.AvroSpecFun;
import jsonvalues.spec.JsSpec;
import jsonvalues.spec.JsonToAvro;
import jsonvalues.spec.SpecToAvroSchema;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

/**
 * Thread-safe class for serializing JSON objects to Avro binary or JSON format based on a provided Avro schema.
 */
public final class SpecSerializer {

  final JsSpec spec;
  final Schema schema;
  final BinaryEncoder reused;
  final EncoderFactory factory;
  final GenericDatumWriter<GenericContainer> writer;

  final boolean isJFREnabled;

  SpecSerializer(boolean isJFREnabled,
                 JsSpec spec,
                 BinaryEncoder reused,
                 EncoderFactory factory
                ) {
    this.isJFREnabled = isJFREnabled;
    this.schema = SpecToAvroSchema.convert(spec);
    this.spec = spec;
    this.reused = reused;
    this.factory = factory;
    this.writer = new GenericDatumWriter<>(schema);

  }


  /**
   * Serializes the given JSON object to Avro binary format based on the provided schema.
   *
   * @param json The JSON object to be serialized.
   * @return A byte array representing the Avro binary encoding of the JSON object.
   */
  public byte[] serialize(final Json<?> json) {

    if (isJFREnabled) {
      var event = new SerializerEvent();
      event.begin();
      try {
        var result = binaryEncode(json);
        event.result = SerializerEvent.RESULT.SUCCESS.name();
        event.bytes = result.length;
        return result;
      } catch (Exception e) {
        event.result = SerializerEvent.RESULT.FAILURE.name();
        event.exception = AvroSpecFun.findUltimateCause(e)
                                     .toString();
        throw e;
      } finally {
        event.end();
        if (event.shouldCommit()) {
          event.commit();
        }
      }
    } else {
      return binaryEncode(json);
    }
  }

  private byte[] binaryEncode(final Json<?> json) {
    assert spec.test(json)
               .isEmpty() :
        "The json object doesn't conform the spec. Errors: %s".formatted(spec.test(json));

    try {
      GenericContainer record = JsonToAvro.convert(json,
                                                   spec);
      try (var stream = new ByteArrayOutputStream()) {
        var encoder = factory.binaryEncoder(stream,
                                            reused);
        writer.write(record,
                     encoder);
        encoder.flush();
        return stream.toByteArray();
      }
    } catch (IOException e) {
      throw new AvroRuntimeException(e);
    }
  }


}
