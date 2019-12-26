from os.path import basename, splitext

import simplejson as json
from yaml import safe_load

from . import Writer


class FileReader(object):
    def __init__(self, writer):
        self.writer = writer

    def process(self, fname, **doc_info):
        self.writer.start(doc_info, fname)
        with open(fname) as f:
            content = f.read()
            prefix, content0 = content.split('\n\n', 1)
            if content0:
                doc_info0 = safe_load(prefix)
                if isinstance(doc_info0, dict) and 'doc_id' in doc_info0:
                    content = content0
                    doc_info.update(doc_info0)
        pos = 0
        for num, para_text in enumerate(content.split('\n')):
            if para_text.strip():
                para_info = {
                    'para_id': num,
                    'para_offset': pos
                }
                self.writer.process_para(para_text.strip(), para_info)
            pos += len(para_text) + 1
        self.writer.end()


class FileWriter(Writer):
    def __init__(self, processor):
        self.processor = processor

    def start(self, doc_info, fname):
        super(FileWriter, self).start(doc_info, fname)
        self.analysis = []

    def process_para(self, para_text, para_info):
        data = self.processor.process_para(para_text)
        data['para_info'] = para_info
        self.analysis.append(data)

    def end(self):
        base = splitext(basename(self.fname))[0]
        dest_fname = ".".join((base, self.processor.model_name, "json"))
        result = {
            "doc_info": self.doc_info,
            "paragraphs": self.analysis
        }
        with open(dest_fname, 'w') as f:
            json.dump(result, f, encoding='utf-8')
