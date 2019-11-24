import simplejson as json
from pykafka import KafkaClient
from pykafka.common import CompressionType, OffsetType

from . import Writer


class KafkaParaWriter(Writer):
    def __init__(self, dest_topic, zookeeper=None, kafka=None, compression='NONE'):
        self.client = KafkaClient(zookeeper_hosts=zookeeper, hosts=kafka)
        dest_topic = self.client.topics[dest_topic]
        compression = getattr(CompressionType, compression)
        self.producer = self.make_producer(dest_topic, compression)

    def make_producer(self, topic, compression):
        return topic.get_sync_producer(compression=compression)

    def process_para(self, sentence, para_info):        
        # maybe try with async?
        para_info.update(self.doc_info)
        data = {
            "para_info": para_info,
            "text": sentence
        }
        self.write(data, self.get_key(para_info))

    def get_key(self, para_info):
        return "%s_%s" % (para_info['docid'], str(para_info['para_id']))

    def write(self, data, key):
        self.producer.produce(json.dumps(data).encode('utf-8'), key.encode('ascii'))



class KafkaAnalysisWriter(KafkaParaWriter):
    def __init__(self, processor, dest_topic, zookeeper=None, kafka=None, compression='NONE'):
        super(KafkaAnalysisWriter, self).__init__(dest_topic, zookeeper, kafka, compression)
        self.processor = processor

    def get_key(self, para_info):
        return super(KafkaAnalysisWriter, self).get_key(para_info) + "_" + self.processor.model_name

    def process_para(self, sentence, para_info):
        data = self.processor.process_para(para_text)
        para_info.update(self.doc_info)
        data['para_info'] = para_info
        self.write(data, self.get_key(para_info))



class KafkaProcessor(KafkaAnalysisWriter):
    def __init__(self, processor, source_topic, dest_topic, zookeeper=None, kafka=None, compression=None):
        self.processor = processor
        super(KafkaProcessor, self).__init__(dest_topic, zookeeper, kafka, compression)
        source_topic = client.topics[source_topic]
        self.consumer = source_topic.get_simple_consumer(
            "spacy_"+processor.model_name,
            auto_commit_enable=True,
            auto_commit_interval_ms=5000)

    def make_producer(self, topic, compression):
        return topic.get_producer(compression=compression)

    def run(self):
        self.producer.start()
        self.consumer.start()
        for msg in self.consumer:
            para = json.loads(msg.value.decode('utf-8'))
            para_info, para_text = para['para_info'], para['para_text']
            self.processor.process_para(para_text, para_info)