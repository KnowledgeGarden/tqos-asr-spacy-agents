import spacy
from datetime import datetime


debug = False

if debug:
    import pdb


class SpacyProcessor(object):

    def __init__(self, model_name, tok_processor=None, span_processor=None):
        self.tok_processor = tok_processor or SpacyProcessor.process_token
        self.span_processor = span_processor or SpacyProcessor.process_span
        self.model_name = model_name
        self.nlp = spacy.load(model_name)

    @staticmethod
    def process_token(tok):
        result = {
            'start': tok.idx,
            'text': tok.text,
        }
        if tok.head != tok:
            result['parent'] = tok.head.idx
        if (tok.left_edge != tok) or (tok.right_edge != tok):
            result['tree_s_idx'] = tok.left_edge.idx
            result['tree_e_idx'] = tok.right_edge.idx
        if tok.pos:
            result['pos'] = tok.pos_
        if tok.tag:
            result['tag'] = tok.tag_
        if tok.dep:
            result['dep'] = tok.dep_
        if tok.lemma_ != tok.text:
            result['lemma'] = tok.lemma_
        if tok.sent:
            result['sent'] = tok.sent.start_char
        if tok.conjuncts:
            result['conjuncts'] = [conj.idx for conj in tok.conjuncts]
        return result

    @staticmethod
    def process_span_sent(span):
        result = {
            'start': span.start_char,
            'end': span.end_char,
            'text': span.text,
        }
        if span.label:
            result['label'] = span.label_
        if span.kb_id_:
            result['kb_id'] = span.kb_id_
        if span.ent_id_:
            result['ent_id'] = span.ent_id_
        return result

    @staticmethod
    def process_span(span):
        result = SpacyProcessor.process_span_sent(span)
        if span.sent:
            result['sent'] = span.sent.start_char
        return result

    @staticmethod
    def process_token_with_vec(tok):
        result = SpacyProcessor.process_token(tok)
        if tok.has_vector:
            result['vector'] = [float(n) for n in tok.vector]
        return result

    def process_para(self, para_text):
        info = self.nlp(para_text)
        result = {
            'time': datetime.now().isoformat(),
            'analyzer': {
                'name': self.nlp.meta['name'],
                'version': self.nlp.meta['version'],
                'lang': self.nlp.meta['lang'],
            },
            'tok_info': [self.tok_processor(tok) for tok in info],
        }
        try:
            result['entities'] = [self.span_processor(s) for s in info.ents]
        except Exception as e:
            if debug:
                print(e)
                pdb.post_mortem()
        try:
            result['sentences'] = [
                SpacyProcessor.process_span_sent(s) for s in info.sents]
        except Exception as e:
            if debug:
                print(e)
                pdb.post_mortem()
        try:
            result['noun_chunks'] = [
                self.span_processor(s) for s in info.noun_chunks]
        except Exception as e:
            if debug:
                print(e)
                pdb.post_mortem()
        return result
