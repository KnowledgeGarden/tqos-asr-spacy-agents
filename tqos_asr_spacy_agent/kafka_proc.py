import simplejson as json
from pykafka import KafkaClient
from pykafka.common import CompressionType, OffsetType
from .processor import SpacyProcessor


class KafkaSpacyProcessor(SpacyProcessor):
    def __init__(self, model_name, source_topic, dest_topic, broker=None, kafka_hosts=None, compression=None):
        super(KafkaSpacyProcessor, self).__init__(model_name)
        client = KafkaClient(hosts=kafka_hosts, zookeeper_hosts=broker)
        source_topic = client.topics[source_topic]
        dest_topic = client.topics[dest_topic]
        compression = getattr(CompressionType, compression) if compression else None
        self.consumer = source_topic.get_simple_consumer(
            "spacy_"+model_name,
            auto_commit_enable=True,
            auto_commit_interval_ms=5000)
        self.producer = dest_topic.get_producer(dest_topic, compression=compression)

    def start(self):
        self.producer.start()
        self.consumer.start()
        for msg in self.consumer:
            para = json.loads(msg.value.decode('utf-8'))
            para_info = para['para_info']
            print("read", para)
            result = self.process_para(para)
            print("writing", result)
            self.producer.produce(json.dumps(result).encode('utf-8'),
                partition_key=("%s_%s_%s" % (para_info['doc_id'], para_info['para_id'], self.model_name)).encode('ascii'))
            


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Add paragraphs to kafka')
    parser.add_argument('--model_name', '-m', type=str,
                        default='en_core_sci_lg',
                        help='Name of spacy model to use')
    parser.add_argument('--source_topic', '-s', type=str,
                        default='paragraphs',
                        help='kafka topic to consume for paragraphs')
    parser.add_argument('--dest_topic', '-d', type=str,
                        default='spacy',
                        help='kafka topic where to put analysis')
    parser.add_argument('--compression', '-c', type=str,
                        default='NONE',
                        help='compression method (GZIP, SNAPPY, LZ4, NONE)')
    parser.add_argument('--broker', '-b', type=str,
                        default='127.0.0.1:2181',
                        help='zookeeper host address and port')
    parser.add_argument('--kafka_hosts', '-k', type=str,
                        default='127.0.0.1:9092',
                        help='zookeeper host address and port')
    args = parser.parse_args()
    p = KafkaSpacyProcessor(args.model_name, args.source_topic, args.dest_topic, args.broker, args.kafka_hosts, args.compression)
    p.start()
