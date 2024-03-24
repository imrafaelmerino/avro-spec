package jsonvalues.spec.deserializers;

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
    this.reusedDecoder = reusedDecoder;
    this.decoderFactory = decoderFactory;
    this.schema = schema;

  }


}
