import faust
from configparser import ConfigParser

config = ConfigParser()
config.read('join_config.ini')
assert 'join' in config
params = config['join']
target_models = set([x.strip() for x in params['models'].split(',')])

app = faust.App(
    'spacy_join',
    broker=params['broker'],
    store=params['store'],
    value_serializer='json',
)
source_topic = app.topic(params['source_topic'],
    partitions = int(params['source_topic_partitions']))
dest_topic = app.topic(params['dest_topic'],
    partitions = int(params['dest_topic_partitions']))

table = app.Table(params['aggregate_table'], default=dict,
    partitions=int(params['source_topic_partitions']))

def analysis_id(model):
    return "_".join((model["para_info"]["doc_id"], model["para_info"]["para_id"]))


@app.agent(source_topic)
async def join(values):
    "Collect values until all models are supplied"
    async for value in values:
        id = analysis_id(value)
        model = value["analyzer"]["name"]
        aggregate = table[id]
        aggregate[model] = value
        if set(aggregate.keys()) == target_models:
            await dest_topic.send(key=id, value=aggregate)
            del table[id]
            print(id, "complete")
        else:
            table[id] = aggregate
            print(id, list(aggregate.keys()))
