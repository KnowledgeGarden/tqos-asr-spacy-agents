import argparse
import asyncio

parser = argparse.ArgumentParser(
    description='Read text from a kafka topic or files; '
    'output analysis to files or kafka topic')
subparsers = parser.add_subparsers(help='sub-command help')
parser_populate = subparsers.add_parser(
    'populate', help='read files and output kafka paragraphs')
parser_pop_analysis = subparsers.add_parser(
    'pop_analysis', help='read files and output kafka analysis')
parser_process = subparsers.add_parser(
    'process', help='read text files and output analysis json')
parser_daemon = subparsers.add_parser(
    'daemon', help='read kafka paragraphs and output kafka analysis')
parser_populate.set_defaults(action='populate')
parser_pop_analysis.set_defaults(action='pop_analysis')
parser_process.set_defaults(action='process')
parser_daemon.set_defaults(action='daemon')

for p in (parser_populate, parser_pop_analysis, parser_daemon):
    p.add_argument('--compression', '-c', type=str,
                   default=None,
                   help='compression method (gzip, snappy, lz4)')
    p.add_argument('--kafka', '-k', type=str,
                   help='kafka host address and port')

parser_daemon.add_argument('--source_topic', '-s', type=str,
                           default='paragraphs',
                           help='kafka topic from where to read paragraphs')
parser_daemon.add_argument('--reset', '-r', action='store_true',
                           help='Read source topic from start')
parser_daemon.add_argument('--hypothesis', action='store_true',
                           help="analyze kafka message as hypothesis json")
parser_populate.add_argument('--dest_topic', '-t', type=str,
                             default='paragraphs',
                             help='kafka topic from where to write paragraphs')
for p in (parser_pop_analysis, parser_daemon):
    p.add_argument('--dest_topic', '-t', type=str,
                   default='spacy',
                   help='kafka topic from where to write analysis')
for p in (parser_pop_analysis, parser_daemon, parser_process):
    p.add_argument('--model_name', '-m', type=str,
                   default='en_core_sci_lg',
                   help='Name of spacy model to use for processing')
for p in (parser_populate, parser_pop_analysis, parser_process):
    p.add_argument('--doc_id', '-d', type=str,
                   default=None,
                   help='document id')
    p.add_argument('--doc_url', '-u', type=str,
                   default=None,
                   help='document url')
    p.add_argument('files', nargs='+', help='input file names')


loop = asyncio.get_event_loop()
args = parser.parse_args()
if args.action != 'populate':
    from .spacy_proc import SpacyProcessor
    processor = SpacyProcessor(args.model_name)
if args.action != 'process':
    if not args.kafka:
        args.kafka = '127.0.0.1:9092'
if args.action == 'daemon':
    if args.hypothesis:
        from .kafka_handlers import HypothesisProcessor as Processor
    else:
        from .kafka_handlers import KafkaProcessor as Processor
    proc = Processor(
            processor, args.source_topic, args.dest_topic, loop,
            args.kafka, args.compression, args.reset)
    loop.run_until_complete(proc.run())
else:
    if len(args.files) > 1 and (args.doc_id or args.doc_url):
        print("Do not specify doc id or url with more than one document")
        exit(1)
    if args.action == 'process':
        from .file_handlers import FileWriter
        writer = FileWriter(processor)
    elif args.action == 'populate':
        from .kafka_handlers import KafkaParaWriter
        writer = KafkaParaWriter(
            args.dest_topic, loop, args.kafka, args.compression)
    elif args.action == 'pop_analysis':
        from .kafka_handlers import KafkaAnalysisWriter
        writer = KafkaAnalysisWriter(
            processor, args.dest_topic, loop, args.kafka,
            args.compression)

    async def process_all():
        from .file_handlers import FileReader
        async with FileReader(writer) as reader:
            for fname in args.files:
                await reader.process(
                    fname, doc_url=args.doc_url, doc_id=args.doc_id or fname)
    loop.run_until_complete(process_all())
