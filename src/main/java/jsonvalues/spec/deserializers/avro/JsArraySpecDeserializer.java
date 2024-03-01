package jsonvalues.spec.deserializers.avro;

import java.nio.charset.StandardCharsets;
import jsonvalues.JsArray;
import jsonvalues.JsObj;
import jsonvalues.spec.AvroSpecFun;
import jsonvalues.spec.AvroToJson;
import jsonvalues.spec.AvroToJsonException;
import jsonvalues.spec.JsArraySpec;
import jsonvalues.spec.MetadataNotFoundException;
import jsonvalues.spec.SpecNotSupportedInAvroException;
import jsonvalues.spec.SpecToAvroSchema;
import jsonvalues.spec.SpecToSchemaException;
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
public final class JsArraySpecDeserializer extends AbstractSpecDeserializer {

  final boolean isJFREnabled;

  final JsArraySpec spec;
  final GenericArray<?> reusedArray;

  final GenericDatumReader<GenericArray<?>> reader;

  JsArraySpecDeserializer(JsArraySpec spec,
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
   * @throws JsSpecDeserializerException If there is an error during deserialization.
   */
  public JsArray binaryDecode(final byte[] json) {
    if (json == null) {
      return null;
    }
    if (isJFREnabled) {
      var event = new AvroDeserializerEvent();
      event.begin();
      try {
        var result = _binaryDecode(json);
        event.result = AvroDeserializerEvent.RESULT.SUCCESS.name();
        return result;
      } catch (Exception e) {
        event.result = AvroDeserializerEvent.RESULT.FAILURE.name();
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
      return _binaryDecode(json);
    }


  }

  private JsArray _binaryDecode(final byte[] json) {
    try {
      var decoder = decoderFactory.binaryDecoder(json,
                                                 reusedDecoder);
      GenericArray<?> record = reader.read(reusedArray,
                                           decoder);
      JsArray decoded = AvroToJson.toJsArray(record);
      assert spec.test(decoded)
                 .isEmpty() :
          "Deserialized json doesn't conform the reader spec of the `AvroSpecDeserializer`. Errors: "
          + spec.test(decoded);
      return decoded;
    } catch (SpecNotSupportedInAvroException | MetadataNotFoundException | SpecToSchemaException |
             AvroToJsonException e) {
      throw e;
    } catch (Exception e) {
      throw new JsSpecDeserializerException(e);
    }
  }

  /**
   * Decodes Avro JSON data into a {@link JsObj} based on the reader specification.
   *
   * @param json The Avro JSON data to decode.
   * @return A {@code JsObj} representing the decoded JSON value.
   * @throws JsSpecDeserializerException If there is an error during deserialization.
   */
  public JsArray jsonDecode(final byte[] json) {
    if (json == null) {
      return null;
    }
    if (isJFREnabled) {
      var event = new AvroDeserializerEvent();
      event.begin();
      try {
        var result = _jsonDecode(json);
        event.result = AvroDeserializerEvent.RESULT.SUCCESS.name();
        return result;
      } catch (Exception e) {
        event.result = AvroDeserializerEvent.RESULT.FAILURE.name();
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
      return _jsonDecode(json);
    }

  }

  private JsArray _jsonDecode(final byte[] json) {
    try {
      var decoder = decoderFactory.jsonDecoder(schema,
                                               new String(json,
                                                          StandardCharsets.UTF_8));
      GenericArray<?> record = reader.read(reusedArray,
                                           decoder);
      JsArray xs = AvroToJson.toJsArray(record);
      assert spec.test(xs)
                 .isEmpty() :
          "Deserialized json doesn't conform the reader spec of the `AvroSpecDeserializer`. Errors: "
          + spec.test(xs);
      return xs;
    } catch (SpecNotSupportedInAvroException | MetadataNotFoundException | SpecToSchemaException |
             AvroToJsonException e) {
      throw e;
    } catch (Exception e) {
      throw new JsSpecDeserializerException(e);
    }
  }


}
