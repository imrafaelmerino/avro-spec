package jsonvalues.spec.serializers.avro;

import java.io.ByteArrayOutputStream;
import java.util.Objects;
import jsonvalues.JsObj;
import jsonvalues.Json;
import jsonvalues.spec.JsSpec;
import jsonvalues.spec.JsonToAvro;
import jsonvalues.spec.JsonToAvroException;
import jsonvalues.spec.MetadataNotFoundException;
import jsonvalues.spec.SpecNotSupportedInAvroException;
import jsonvalues.spec.SpecToAvroSchema;
import jsonvalues.spec.SpecToSchemaException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.jetbrains.annotations.NotNull;

/**
 * Thread-safe class for serializing JSON objects to Avro binary or JSON format based on a provided Avro schema.
 */
public final class JsonSpecSerializer {

  final JsSpec spec;

  final Schema schema;
  final BinaryEncoder reused;
  final EncoderFactory factory;
  final GenericDatumWriter<GenericContainer> writer;

  final boolean isJFREnabled;

  JsonSpecSerializer(boolean isJFREnabled,
                     JsSpec spec,
                     BinaryEncoder reused,
                     EncoderFactory factory
                    ) {
    this.isJFREnabled = isJFREnabled;
    this.schema = SpecToAvroSchema.convert(spec);
    if (!isRecordSchema(schema)) {
      throw JsonSpecSerializerException.invalidSpecForRecords();
    }
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
   * @throws JsonSpecSerializerException If an error occurs during serialization.
   */
  public byte[] binaryEncode(final Json<?> json) {

    if (isJFREnabled) {
      var event = new AvroSerializerEvent();
      event.begin();
      try {
        var result = _binaryEncode(json);
        event.result = AvroSerializerEvent.RESULT.SUCCESS.name();
        event.bytes = result.length;
        return result;
      } catch (Exception e) {
        event.result = AvroSerializerEvent.RESULT.FAILURE.name();
        event.exception = findUltimateCause(e).toString();
        throw e;
      } finally {
        event.end();
        if (event.shouldCommit()) {
          event.commit();
        }
      }
    } else {
      return _binaryEncode(json);
    }
  }

  @NotNull
  private byte[] _binaryEncode(final Json<?> json) {
    assert spec.test(json)
               .isEmpty() :
        "The json object doesn't conform the spec. Errors: %s".formatted(spec.test(json));

    try {
      GenericContainer record = JsonToAvro.toAvro(json,
                                                  spec);
      try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
        var encoder = factory.binaryEncoder(stream,
                                            reused);
        writer.write(record,
                     encoder);
        encoder.flush();
        return stream.toByteArray();
      }
    } catch (MetadataNotFoundException | SpecNotSupportedInAvroException | SpecToSchemaException |
             JsonToAvroException e) {
      throw e;
    } catch (Exception e) {
      throw new JsonSpecSerializerException(e);

    }
  }

  /**
   * Serializes the given JSON object to Avro JSON format based on the provided schema.
   *
   * @param json   The JSON object to be serialized.
   * @param pretty Flag indicating whether to format the JSON output in a pretty-printed style.
   * @return A byte array representing the Avro JSON encoding of the JSON object.
   * @throws JsonSpecSerializerException If an error occurs during serialization.
   */
  public byte[] jsonEncode(final JsObj json,
                           final boolean pretty
                          ) {
    if (isJFREnabled) {
      var event = new AvroSerializerEvent();
      event.begin();
      try {
        var result = _jsonEncode(json,
                                 pretty);
        event.result = AvroSerializerEvent.RESULT.SUCCESS.name();
        event.bytes = result.length;
        return result;
      } catch (Exception e) {
        event.result = AvroSerializerEvent.RESULT.FAILURE.name();
        event.exception = findUltimateCause(e).toString();
        throw e;
      } finally {
        event.end();
        if (event.shouldCommit()) {
          event.commit();
        }
      }
    } else {
      return _jsonEncode(json,
                         pretty);
    }

  }

  private byte[] _jsonEncode(final JsObj json,
                             final boolean pretty) {
    assert spec.test(json)
               .isEmpty() :
        "The json doesn't conform the spec. Errors: %s".formatted(spec.test(json));

    try {
      var record = JsonToAvro.toRecord(json,
                                       schema);
      try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
        var encoder = factory.jsonEncoder(schema,
                                          stream,
                                          pretty);
        writer.write(record,
                     encoder);
        encoder.flush();
        return stream.toByteArray();
      }
    } catch (MetadataNotFoundException | SpecNotSupportedInAvroException | SpecToSchemaException |
             JsonToAvroException e) {
      throw e;
    } catch (Exception e) {
      throw new JsonSpecSerializerException(e);
    }
  }

  private boolean isRecordSchema(Schema schema) {
    return schema.getType() == Schema.Type.RECORD
           || (schema.isUnion()
               && schema.getTypes()
                        .stream()
                        .allMatch(it -> it.getType() == Schema.Type.RECORD)
           );
  }

  /**
   * Finds the ultimate cause in the exception chain or the exception if the cause is null.
   *
   * @param exception The initial exception to start the search from.
   * @return The ultimate cause in the exception chain.
   * @throws NullPointerException If the provided exception is {@code null}.
   */
  private Throwable findUltimateCause(Throwable exception) {
    var ultimateCause = Objects.requireNonNull(exception);

    while (ultimateCause.getCause() != null) {
      ultimateCause = ultimateCause.getCause();
    }

    return ultimateCause;
  }


}
