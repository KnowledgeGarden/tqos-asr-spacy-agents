import asyncio
import simplejson as json
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

from . import Writer


class KafkaParaWriter(Writer):
    def __init__(self, dest_topic, loop, kafka=None, compression=None):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=kafka, loop=loop, compression_type=compression)
        self.dest_topic = dest_topic

    async def __aenter__(self):
        await self.producer.start()
        return await super(KafkaParaWriter, self).__aenter__()

    async def __aexit__(self, exc_type, exc, tb):
        await self.producer.stop()
        await super(KafkaParaWriter, self).__aexit__(exc_type, exc, tb)

    async def process_para(self, para_text, para_info):
        # maybe try with async?
        para_info.update(self.doc_info)
        data = {
            "para_info": para_info,
            "text": para_text
        }
        await self.write(data, self.get_key(para_info))

    def get_key(self, para_info):
        return "%s_%s" % (para_info['doc_id'], str(para_info['para_id']))

    async def write(self, data, key):
        await self.producer.send_and_wait(
            self.dest_topic,
            json.dumps(data).encode('utf-8'),
            key=key.encode('ascii'))


class KafkaAnalysisWriter(KafkaParaWriter):
    def __init__(self, processor, dest_topic, loop, kafka=None,
                 compression=None):
        super(KafkaAnalysisWriter, self).__init__(
            dest_topic, loop, kafka, compression)
        self.processor = processor

    def get_key(self, para_info):
        return "_".join(
            (super(KafkaAnalysisWriter, self).get_key(para_info),
             self.processor.model_name))

    async def process_para(self, para_text, para_info):
        data = self.processor.process_para(para_text)
        para_info.update(self.doc_info)
        data['para_info'] = para_info
        await self.write(data, self.get_key(para_info))


class KafkaProcessor(KafkaAnalysisWriter):
    def __init__(self, processor, source_topic, dest_topic, loop,
                 kafka=None, compression=None, reset=False):
        super(KafkaProcessor, self).__init__(
            processor, dest_topic, loop, kafka, compression)
        self.doc_info = {}
        self.reset = reset
        self.consumer = AIOKafkaConsumer(
            source_topic,
            loop=loop, bootstrap_servers=kafka,
            auto_offset_reset='earliest' if reset else 'latest',
            group_id="spacy_"+processor.model_name)

    async def __aenter__(self):
        await self.consumer.start()
        if self.reset:
            await self.consumer.seek_to_beginning()
        return await super(KafkaProcessor, self).__aenter__()

    async def __aexit__(self, exc_type, exc, tb):
        await self.consumer.stop()
        await super(KafkaProcessor, self).__aexit__(exc_type, exc, tb)

    async def run(self):
        async with self as proc:
            async for msg in proc.consumer:
                print(msg)
                data = json.loads(msg.value.decode('utf-8'))
                await proc.process(data)

    async def process(self, data):
        para_info, para_text = data['para_info'], data['text']
        await self.process_para(para_text, para_info)


class HypothesisProcessor(KafkaProcessor):
    async def process(self, data):
        id = data['id']
        for k in ('annotation', "text"):
            para = data.get(k, None)
            if para:
                await self.process_para(para, {
                    "doc_id": "hyp_" + id, "para_id": k, "hyp": data})
