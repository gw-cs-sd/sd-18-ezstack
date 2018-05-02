# Denormalizer

The denormalizer is the only component of EZstack that is able to write directly into Elasticsearch.

## Inputs

The main input to the denormalizer is the `documents` Kafka stream, which is a stream of updates from EZapp.  