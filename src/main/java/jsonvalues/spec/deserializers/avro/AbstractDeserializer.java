package jsonvalues.spec.deserializers.avro;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

abstract class AbstractDeserializer {

  final DecoderFactory decoderFactory;

  final BinaryDecoder reusedDecoder;

  public AbstractDeserializer(final DecoderFactory decoderFactory,
                              final BinaryDecoder reusedDecoder) {
    this.decoderFactory = decoderFactory;
    this.reusedDecoder = reusedDecoder;
  }


}
