
class Writer(object):
    def setup(self, doc_info, fname):
        self.doc_info = doc_info
        self.fname = fname

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    async def process_para(self, sentence, para_info):
        pass
