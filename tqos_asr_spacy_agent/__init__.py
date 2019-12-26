
class Writer(object):
    def start(self, doc_info, fname):
        self.doc_info = doc_info
        self.fname = fname

    def end(self):
        pass

    def process_para(self, sentence, para_info):
        pass
