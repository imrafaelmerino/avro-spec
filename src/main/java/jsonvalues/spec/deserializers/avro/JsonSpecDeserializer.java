package jsonvalues.spec.deserializers.avro;

import java.nio.charset.StandardCharsets;
import jsonvalues.JsArray;
import jsonvalues.JsObj;
import jsonvalues.Json;
import jsonvalues.spec.AvroSpecFun;
import jsonvalues.spec.AvroToJson;
import jsonvalues.spec.AvroToJsonException;
import jsonvalues.spec.JsSpec;
import jsonvalues.spec.MetadataNotFoundException;
import jsonvalues.spec.SpecNotSupportedInAvroException;
import jsonvalues.spec.SpecToAvroSchema;
import jsonvalues.spec.SpecToSchemaException;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
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
public final class JsonSpecDeserializer extends AbstractSpecDeserializer {

  final boolean isJFREnabled;

  final JsSpec spec;
  final GenericDatumReader<GenericRecord> reader;


  JsonSpecDeserializer(JsSpec spec,
                       DecoderFactory decoderFactory,
                       BinaryDecoder reusedDecoder,
                       boolean isJFREnabled
                      ) {
    super(SpecToAvroSchema.convert(spec),
          decoderFactory,
          reusedDecoder
         );
    this.spec = spec;
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
  public Json<?> binaryDecode(final byte[] json) {
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

  private Json<?> _binaryDecode(final byte[] json) {
    try {
      var decoder = decoderFactory.binaryDecoder(json,
                                                 reusedDecoder);
      GenericContainer container = reader.read(null,
                                               decoder);
      if (container instanceof GenericRecord record) {
        JsObj decoded = AvroToJson.toJsObj(record);
        assert spec.test(decoded)
                   .isEmpty() :
            "Deserialized json object doesn't conform the reader spec of the `AvroSpecDeserializer`. Errors: %s".formatted(spec.test(decoded));
        return decoded;
      } else if (container instanceof GenericArray<?> array) {
        JsArray decoded = AvroToJson.toJsArray(array);
        assert spec.test(decoded)
                   .isEmpty() :
            "Deserialized json array doesn't conform the reader spec of the `AvroSpecDeserializer`. Errors: %s".formatted(spec.test(decoded));
        return decoded;
      } else {
        throw new JsSpecDeserializerException("The deserialized object is neither a record nor an array");
      }
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
  public Json<?> jsonDecode(final byte[] json) {

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

  private Json<?> _jsonDecode(final byte[] json) {
    try {
      var decoder = decoderFactory.jsonDecoder(schema,
                                               new String(json,
                                                          StandardCharsets.UTF_8));
      GenericContainer container = reader.read(null,
                                               decoder);
      if (container instanceof GenericRecord record) {
        JsObj decoded = AvroToJson.toJsObj(record);
        assert spec.test(decoded)
                   .isEmpty() :
            "Deserialized json object doesn't conform the reader spec. Errors: %s".formatted(spec.test(decoded));
        return decoded;
      }
      if (container instanceof GenericArray<?> array) {
        JsArray decoded = AvroToJson.toJsArray(array);
        assert spec.test(decoded)
                   .isEmpty() :
            "Deserialized json array doesn't conform the reader spec. Errors: %s".formatted(spec.test(decoded));
        return decoded;
      } else {
        throw new JsSpecDeserializerException("The deserialized object is neither a record nor an array");
      }
    } catch (SpecNotSupportedInAvroException | MetadataNotFoundException | SpecToSchemaException |
             AvroToJsonException e) {
      throw e;
    } catch (Exception e) {
      throw new JsSpecDeserializerException(e);
    }
  }


}
