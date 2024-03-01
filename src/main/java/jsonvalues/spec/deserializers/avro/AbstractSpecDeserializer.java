package jsonvalues.spec.deserializers.avro;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

abstract class AbstractSpecDeserializer {

  final Schema schema;

  final DecoderFactory decoderFactory;

  final BinaryDecoder reusedDecoder;

  public AbstractSpecDeserializer(final Schema schema,
                                  final DecoderFactory decoderFactory,
                                  final BinaryDecoder reusedDecoder) {
    this.schema = schema;
    if (isNotRecord(this.schema)) {
      throw JsSpecDeserializerException.invalidSpecForRecords();
    }
    this.decoderFactory = decoderFactory;
    this.reusedDecoder = reusedDecoder;
  }

  boolean isNotRecord(Schema schema) {
    return schema.getType() != Schema.Type.RECORD
           && (!schema.isUnion()
               || !schema.getTypes()
                         .stream()
                         .allMatch(it -> it.getType() == Schema.Type.RECORD));
  }
}
