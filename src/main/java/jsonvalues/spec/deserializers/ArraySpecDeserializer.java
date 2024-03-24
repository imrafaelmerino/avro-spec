package jsonvalues.spec.deserializers;

import java.io.IOException;
import jsonvalues.JsArray;
import jsonvalues.JsObj;
import jsonvalues.spec.AvroSpecFun;
import jsonvalues.spec.AvroToJson;
import jsonvalues.spec.JsArraySpec;
import jsonvalues.spec.SpecToAvroSchema;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

/**
 * Thread-safe class for converting Avro binary or JSON-encoded data to JSON values based on reader and writer
 * specifications.
 * <p>
 * This class provides methods to decode Avro binary or JSON data into JSON values using the specified reader and writer
 * specifications. It ensures that the decoded JSON values conform to the reader specification.
 * </p>
 */
public final class ArraySpecDeserializer extends AbstractSpecDeserializer {

  final boolean isJFREnabled;

  final JsArraySpec spec;
  final GenericArray<?> reusedArray;

  final GenericDatumReader<GenericArray<?>> reader;

  ArraySpecDeserializer(JsArraySpec spec,
                        GenericArray<?> reusedArray,
                        DecoderFactory decoderFactory,
                        BinaryDecoder reusedDecoder,
                        boolean isJFREnabled
                       ) {
    super(SpecToAvroSchema.convert(spec),
          decoderFactory,
          reusedDecoder);
    this.spec = spec;
    this.reusedArray = reusedArray;
    this.isJFREnabled = isJFREnabled;
    this.reader = new GenericDatumReader<>(schema);

  }


  /**
   * Decodes Avro binary data into a {@link JsObj} based on the reader specification.
   *
   * @param json The Avro binary data to decode.
   * @return A {@code JsObj} representing the decoded JSON value.
   */
  public JsArray deserialize(final byte[] json) {
    if (json == null) {
      return null;
    }
    if (isJFREnabled) {
      var event = new DeserializerEvent();
      event.begin();
      try {
        var result = binaryDecode(json);
        event.result = DeserializerEvent.RESULT.SUCCESS.name();
        return result;
      } catch (Exception e) {
        event.result = DeserializerEvent.RESULT.FAILURE.name();
        event.exception = AvroSpecFun.findUltimateCause(e)
                                     .toString();
        throw e;
      } finally {
        event.end();
        if (event.shouldCommit()) {
          event.bytes = json.length;
          event.commit();
        }
      }
    } else {
      return binaryDecode(json);
    }


  }

  private JsArray binaryDecode(final byte[] json) {
    try {
      var decoder = decoderFactory.binaryDecoder(json,
                                                 reusedDecoder);
      GenericArray<?> record = reader.read(reusedArray,
                                           decoder);
      JsArray decoded = AvroToJson.convert(record);
      assert spec.test(decoded)
                 .isEmpty() :
          "Deserialized json doesn't conform the reader spec of the `AvroSpecDeserializer`. Errors: "
          + spec.test(decoded);
      return decoded;
    } catch (IOException e) {
      throw new AvroRuntimeException(e);
    }
  }


}
