import simplejson as json
from pykafka import KafkaClient
from pykafka.common import CompressionType

from . import Writer


class KafkaParaWriter(Writer):
    def __init__(self, dest_topic, zookeeper=None, kafka=None,
                 compression='NONE'):
        self.client = KafkaClient(zookeeper_hosts=zookeeper, hosts=kafka)
        dest_topic = self.client.topics[dest_topic]
        compression = getattr(CompressionType, compression)
        self.producer = self.make_producer(dest_topic, compression)

    def make_producer(self, topic, compression):
        return topic.get_sync_producer(compression=compression)

    def end(self):
        self.producer.stop()

    def process_para(self, para_text, para_info):
        # maybe try with async?
        para_info.update(self.doc_info)
        data = {
            "para_info": para_info,
            "text": para_text
        }
        self.write(data, self.get_key(para_info))

    def get_key(self, para_info):
        return "%s_%s" % (para_info['doc_id'], str(para_info['para_id']))

    def write(self, data, key):
        self.producer.produce(json.dumps(data).encode('utf-8'), key.encode('ascii'))


class KafkaAnalysisWriter(KafkaParaWriter):
    def __init__(self, processor, dest_topic, zookeeper=None, kafka=None,
                 compression='NONE'):
        super(KafkaAnalysisWriter, self).__init__(
            dest_topic, zookeeper, kafka, compression)
        self.processor = processor

    def get_key(self, para_info):
        return "_".join(
            (super(KafkaAnalysisWriter, self).get_key(para_info),
             self.processor.model_name))

    def process_para(self, para_text, para_info):
        data = self.processor.process_para(para_text)
        para_info.update(self.doc_info)
        data['para_info'] = para_info
        self.write(data, self.get_key(para_info))


class KafkaProcessor(KafkaAnalysisWriter):
    def __init__(self, processor, source_topic, dest_topic, zookeeper=None,
                 kafka=None, compression=None, reset=False):
        super(KafkaProcessor, self).__init__(
            processor, dest_topic, zookeeper, kafka, compression)
        source_topic = self.client.topics[source_topic]
        self.doc_info = {}
        self.consumer = source_topic.get_simple_consumer(
            "spacy_"+processor.model_name,
            auto_commit_enable=True,
            auto_commit_interval_ms=5000,
            reset_offset_on_start=reset)

    def make_producer(self, topic, compression):
        return topic.get_producer(compression=compression)

    def end(self):
        super(KafkaProcessor, self).end()
        self.consumer.stop()

    def run(self):
        self.producer.start()
        self.consumer.start()
        for msg in self.consumer:
            print(msg)
            data = json.loads(msg.value.decode('utf-8'))
            self.process(data)

    def process(self, data):
        para_info, para_text = data['para_info'], data['text']
        self.process_para(para_text, para_info)


class HypothesisProcessor(KafkaProcessor):
    def process(self, data):
        id = data['id']
        for k in ('annotation', "text"):
            para = data.get(k, None)
            if para:
                self.process_para(para, {
                    "doc_id": "hyp_" + id, "para_id": k, "hyp": data})
