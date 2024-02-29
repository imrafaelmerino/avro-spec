package jsonvalues.spec;

import static jsonvalues.spec.AvroFun.isRecordSchema;

import java.nio.charset.StandardCharsets;
import jsonvalues.JsObj;
import org.apache.avro.Schema;
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
public final class ObjSpecDeserializer {

  final JsSpec readerSpec;
  final JsSpec writerSpec;

  final Schema readerSchema;
  final Schema writerSchema;

  final GenericRecord reusedRecord;

  final DecoderFactory decoderFactory;

  final BinaryDecoder reusedDecoder;

  final GenericDatumReader<GenericRecord> reader;


  ObjSpecDeserializer(JsSpec readerSpec,
                      JsSpec writerSpec,
                      GenericRecord reusedRecord,
                      DecoderFactory decoderFactory,
                      BinaryDecoder reusedDecoder
                     ) {
    this.readerSpec = readerSpec;
    this.writerSpec = writerSpec;
    this.reusedRecord = reusedRecord;
    this.decoderFactory = decoderFactory;
    this.reusedDecoder = reusedDecoder;
    this.readerSchema = SpecToAvroSchema.convert(readerSpec);
    this.writerSchema = SpecToAvroSchema.convert(writerSpec);
    if (!isRecordSchema(readerSchema)) {
      throw SpecDeserializerException.invalidSpecForRecords();
    }
    if (!isRecordSchema(writerSchema)) {
      throw SpecDeserializerException.invalidSpecForRecords();
    }
    this.reader = new GenericDatumReader<>(writerSchema,
                                           readerSchema);

  }


  /**
   * Decodes Avro binary data into a {@link JsObj} based on the reader specification.
   *
   * @param json The Avro binary data to decode.
   * @return A {@code JsObj} representing the decoded JSON value.
   * @throws SpecDeserializerException If there is an error during deserialization.
   */
  public JsObj binaryDecode(final byte[] json) {

    try {
      var decoder = decoderFactory.binaryDecoder(json,
                                                 reusedDecoder);
      GenericRecord record = reader.read(reusedRecord,
                                         decoder);
      JsObj decoded = AvroToJson.toJsObj(record);
      assert readerSpec.test(decoded)
                       .isEmpty() :
          "Deserialized json doesn't conform the reader spec of the `AvroSpecDeserializer`. Errors: "
          + readerSpec.test(decoded);
      return decoded;
    } catch (SpecNotSupportedInAvroException | MetadataNotFoundException | SpecToSchemaException |
             AvroToJsonException e) {
      throw e;
    } catch (Exception e) {
      throw new SpecDeserializerException(e);
    }

  }

  /**
   * Decodes Avro JSON data into a {@link JsObj} based on the reader specification.
   *
   * @param json The Avro JSON data to decode.
   * @return A {@code JsObj} representing the decoded JSON value.
   * @throws SpecDeserializerException If there is an error during deserialization.
   */
  public JsObj jsonDecode(final byte[] json) {
    try {
      var decoder = decoderFactory.jsonDecoder(readerSchema,
                                               new String(json,
                                                          StandardCharsets.UTF_8));
      GenericRecord record = reader.read(reusedRecord,
                                         decoder);
      JsObj xs = AvroToJson.toJsObj(record);
      assert readerSpec.test(xs)
                       .isEmpty() :
          "Deserialized json doesn't conform the reader spec of the `AvroSpecDeserializer`. Errors: "
          + readerSpec.test(xs);
      return xs;
    } catch (SpecNotSupportedInAvroException | MetadataNotFoundException | SpecToSchemaException |
             AvroToJsonException e) {
      throw e;
    } catch (Exception e) {
      throw new SpecDeserializerException(e);
    }

  }


}
