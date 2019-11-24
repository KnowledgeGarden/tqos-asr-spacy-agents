import argparse
parser = argparse.ArgumentParser(description='Read text from a kafak topic or files; output analysis to files or kafka topic')
subparsers = parser.add_subparsers(help='sub-command help')
parser_populate = subparsers.add_parser('populate', help='read files and output kafka paragraphs')
parser_pop_analysis = subparsers.add_parser('pop_analysis', help='read files and output kafka analysis')
parser_process = subparsers.add_parser('process', help='read text files and output analysis json')
parser_daemon = subparsers.add_parser('daemon', help='read kafka paragraphs and output kafka analysis')
parser_populate.set_defaults(action='populate')
parser_pop_analysis.set_defaults(action='pop_analysis')
parser_process.set_defaults(action='process')
parser_daemon.set_defaults(action='daemon')

for p in (parser_populate, parser_pop_analysis, parser_daemon):
    p.add_argument('--compression', '-c', type=str,
                        default='NONE',
                        help='compression method (GZIP, SNAPPY, LZ4, NONE)')
    p.add_argument('--kafka', '-k', type=str,
                        default='127.0.0.1:9092',
                        help='kafka host address and port')
    p.add_argument('--zookeeper', '-z', type=str,
                        default='127.0.0.1:2181',
                        help='zookeeper host address and port')

parser_daemon.add_argument('--source_topic', '-s', type=str,
                        default='paragraphs',
                        help='kafka topic from where to read paragraphs')
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
    p.add_argument('--docid', '-d', type=str,
                        default=None,
                        help='document id')
    p.add_argument('--doc_url', '-u', type=str,
                        default=None,
                        help='document url')
    p.add_argument('files', nargs='+', help='input file names')

args = parser.parse_args()
if args.action != 'populate':
    from .spacy_proc import SpacyProcessor
    processor = SpacyProcessor(args.model_name)
if args.action == 'daemon':
    from .kafka_handlers import KafkaProcessor
    proc = KafkaProcessor(processor, args.source_topic, args.dest_topic, args.zookeeper, args.kafka, args.compression)
    proc.run()
    exit(0)
else:
    if len(args.files) > 1 and (args.doc_id or args.doc_url):
        print("Do not specify doc id or url with more than one document")
        exit(1)
    if args.action == 'process':
        from .file_handlers import FileWriter
        writer = FileWriter(processor)
    elif args.action == 'populate':
        from .kafka_handlers import KafkaParaWriter
        writer = KafkaParaWriter(args.dest_topic, args.zookeeper, args.kafka, args.compression)
    elif args.action == 'pop_analysis':
        from .kafka_handlers import KafkaProcWriter
        writer = KafkaAnalysisWriter(processor, args.dest_topic, args.zookeeper, args.kafka, args.compression)
    from .file_handlers import FileReader
    reader = FileReader(writer)
    for fname in args.files:
        reader.process(fname, doc_url=args.doc_url, docid=args.docid or fname)
