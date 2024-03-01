package jsonvalues.spec.deserializers.avro;

import java.nio.charset.StandardCharsets;
import jsonvalues.JsObj;
import jsonvalues.spec.AvroSpecFun;
import jsonvalues.spec.AvroToJson;
import jsonvalues.spec.AvroToJsonException;
import jsonvalues.spec.JsSpec;
import jsonvalues.spec.MetadataNotFoundException;
import jsonvalues.spec.SpecNotSupportedInAvroException;
import jsonvalues.spec.SpecToAvroSchema;
import jsonvalues.spec.SpecToSchemaException;
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
public final class JsObjSpecDeserializer extends AbstractSpecDeserializer {

  final boolean isJFREnabled;

  final JsSpec spec;
  final GenericRecord reusedRecord;
  final GenericDatumReader<GenericRecord> reader;


  JsObjSpecDeserializer(JsSpec spec,
                        GenericRecord reusedRecord,
                        DecoderFactory decoderFactory,
                        BinaryDecoder reusedDecoder,
                        boolean isJFREnabled
                       ) {
    super(SpecToAvroSchema.convert(spec),
          decoderFactory,
          reusedDecoder
         );
    this.spec = spec;
    this.reusedRecord = reusedRecord;
    this.isJFREnabled = isJFREnabled;
    this.reader = new GenericDatumReader<>(schema,
                                           schema);
  }


  /**
   * Decodes Avro binary data into a {@link JsObj} based on the reader specification.
   *
   * @param json The Avro binary data to decode.
   * @return A {@code JsObj} representing the decoded JSON value.
   * @throws JsSpecDeserializerException If there is an error during deserialization.
   */
  public JsObj binaryDecode(final byte[] json) {
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

  private JsObj _binaryDecode(final byte[] json) {
    try {
      var decoder = decoderFactory.binaryDecoder(json,
                                                 reusedDecoder);
      GenericRecord record = reader.read(reusedRecord,
                                         decoder);
      JsObj decoded = AvroToJson.toJsObj(record);
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
  public JsObj jsonDecode(final byte[] json) {
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

  private JsObj _jsonDecode(final byte[] json) {
    try {
      var decoder = decoderFactory.jsonDecoder(schema,
                                               new String(json,
                                                          StandardCharsets.UTF_8));
      GenericRecord record = reader.read(reusedRecord,
                                         decoder);
      JsObj xs = AvroToJson.toJsObj(record);
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
