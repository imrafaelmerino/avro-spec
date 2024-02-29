package jsonvalues.spec;

import static jsonvalues.spec.AvroFun.isRecordSchema;

import java.nio.charset.StandardCharsets;
import jsonvalues.JsArray;
import jsonvalues.JsObj;
import org.apache.avro.Schema;
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
public final class ArraySpecDeserializer {

  final JsArraySpec readerSpec;
  final JsArraySpec writerSpec;

  final Schema readerSchema;
  final Schema writerSchema;

  final GenericArray<?> reusedRecord;

  final DecoderFactory decoderFactory;

  final BinaryDecoder reusedDecoder;

  final GenericDatumReader<GenericArray<?>> reader;

  final String name;


  ArraySpecDeserializer(String name,
                        JsArraySpec readerSpec,
                        JsArraySpec writerSpec,
                        GenericArray<?> reusedRecord,
                        DecoderFactory decoderFactory,
                        BinaryDecoder reusedDecoder
                       ) {
    this.name = name;
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
  public JsArray binaryDecode(final byte[] json) {
    try {
      var decoder = decoderFactory.binaryDecoder(json,
                                                 reusedDecoder);
      GenericArray<?> record = reader.read(reusedRecord,
                                           decoder);
      JsArray decoded = AvroToJson.toJsArray(record);
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
  public JsArray jsonDecode(final byte[] json) {
    try {
      var decoder = decoderFactory.jsonDecoder(readerSchema,
                                               new String(json,
                                                          StandardCharsets.UTF_8));
      GenericArray<?> record = reader.read(reusedRecord,
                                           decoder);
      JsArray xs = AvroToJson.toJsArray(record);
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
