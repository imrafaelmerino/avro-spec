package jsonvalues.spec;

import jsonvalues.JsObj;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;

import static jsonvalues.spec.AvroUtils.isRecordSchema;

/**
 * Thread-safe class for serializing JSON objects to Avro binary or JSON format based on a provided Avro schema.
 */
public final class SpecSerializer {

    final String name;

    final JsSpec spec;
    final Schema schema;
    final BinaryEncoder reused;
    final EncoderFactory factory;
    final GenericDatumWriter<GenericRecord> writer;

    final boolean enableDebug;

    SpecSerializer(String serializerName,
                   JsSpec spec,
                   BinaryEncoder reused,
                   EncoderFactory factory,
                   boolean enableDebug
                  ) {
        this.name = serializerName;
        this.enableDebug = enableDebug;
        assert !enableDebug || name != null;
        this.schema = SpecToSchema.convert(spec);
        if (!isRecordSchema(schema)) throw SpecSerializerException.invalidSpecForRecords();
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
     * @throws SpecSerializerException If an error occurs during serialization.
     */
    public byte[] binaryEncode(final JsObj json) {

        assert spec.test(json).isEmpty() :
                "The json object doesn't conform the spec. Errors: %s".formatted(spec.test(json));

        var event = start();
        try {
            var record = JsonToAvro.toRecord(json, schema);
            try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
                var encoder = factory.binaryEncoder(stream, reused);
                writer.write(record, encoder);
                encoder.flush();
                byte[] xs = stream.toByteArray();
                end(event);
                return xs;
            }
        } catch (MetadataNotFoundException | SpecNotSupportedInAvroException | SpecToSchemaException |
                 JsonToAvroException e) {
            end(event, e);
            throw e;
        } catch (Exception e) {
            end(event, e);
            throw new SpecSerializerException(e);

        }


    }


    private SpecSerializerEvent start() {
        if (enableDebug) {
            assert name != null;
            var event = new SpecSerializerEvent(name);
            event.begin();
            return event;
        } else return null;
    }

    private void end(SpecSerializerEvent event) {
        if (enableDebug) {
            assert name != null;
            event.registerSuccess();
            event.commit();
        }

    }

    private void end(SpecSerializerEvent event, Exception e) {
        if (enableDebug) {
            assert name != null;
            event.registerError(e);
            event.commit();
        }

    }

    /**
     * Serializes the given JSON object to Avro JSON format based on the provided schema.
     *
     * @param json   The JSON object to be serialized.
     * @param pretty Flag indicating whether to format the JSON output in a pretty-printed style.
     * @return A byte array representing the Avro JSON encoding of the JSON object.
     * @throws SpecSerializerException If an error occurs during serialization.
     */
    public byte[] jsonEncode(final JsObj json,
                             final boolean pretty
                            ) {
        assert spec.test(json).isEmpty() :
                "The json doesn't conform the spec. Errors: %s".formatted(spec.test(json));

        var event = start();

        try {
            var record = JsonToAvro.toRecord(json, schema);
            try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
                var encoder = factory.jsonEncoder(schema, stream, pretty);
                writer.write(record, encoder);
                encoder.flush();
                byte[] xs = stream.toByteArray();
                end(event);
                return xs;
            }
        } catch (MetadataNotFoundException | SpecNotSupportedInAvroException | SpecToSchemaException |
                 JsonToAvroException e) {
            end(event);
            throw e;
        } catch (Exception e) {
            end(event,e);
            throw new SpecSerializerException(e);

        }
    }


}
