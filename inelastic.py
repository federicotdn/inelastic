#!/usr/bin/env python3

import argparse
import sys
import json
from collections import defaultdict
from elasticsearch import Elasticsearch, ElasticsearchException

SEARCH_SIZE = 50
SCROLL_TIME = '1m'
CSV_SEP = ','
CSV_EOL = '\n'


class InvertedIndex:
    def __init__(self):
        self._max_term_freq = 0
        self._term_dict = defaultdict(list)
        self._dirty = True

    def add_terms(self, doc_id, terms):
        for term in terms:
            ids = self._term_dict[term]
            ids.append(doc_id)
            self._max_term_freq = max(self._max_term_freq, len(ids))

        self._dirty = True

    def _sort_ids(self):
        if not self._dirty:
            return

        for ids in self._term_dict.values():
            ids.sort()

        self._dirty = False

    def write_csv(self, fp):
        self._sort_ids()

        fields = ['term', 'freq']
        fields.extend('d{}'.format(i) for i in range(self._max_term_freq))
        fp.write('{}{}'.format(CSV_SEP.join(fields), CSV_EOL))

        sorted_terms = sorted(self._term_dict.items(),
                              key=lambda i: i[0])

        for term, ids in sorted_terms:
            fp.write('{}{}'.format(term, CSV_SEP))
            fp.write('{}{}'.format(len(ids), CSV_SEP))
            fp.write('{}{}'.format(CSV_SEP.join(ids), CSV_EOL))

    def write_json(self, fp):
        self._sort_ids()

        sorted_terms = sorted(self._term_dict.items(),
                              key=lambda i: i[0])

        obj = {
            'terms': [{
                'term': term,
                'freq': len(ids),
                'ids': ids
            } for term, ids in sorted_terms]
        }

        json.dump(obj, fp, indent=4, ensure_ascii=False)


def get_inverted_index(es, index, doc_type, field):
    search = es.search(index=index, scroll=SCROLL_TIME, _source=False, body={
        'size': SEARCH_SIZE
    })

    inv_index = InvertedIndex()

    while search['hits']['hits']:
        hits = search['hits']['hits']
        ids = [hit['_id'] for hit in hits]

        resp = es.mtermvectors(ids=ids, index=index, doc_type=doc_type,
                               fields=field)

        for result in resp['docs']:
            doc_id = result['_id']
            terms = result['term_vectors'][field]['terms'].keys()
            inv_index.add_terms(doc_id, terms)

        scroll_id = search['_scroll_id']
        search = es.scroll(scroll_id, scroll=SCROLL_TIME)

    es.clear_scroll(scroll_id)
    return inv_index


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--index', metavar='<name>', required=True)
    parser.add_argument('-e', '--host', metavar='<host>', default='localhost')
    parser.add_argument('-p', '--port', metavar='<port>', default=9200)
    parser.add_argument('-d', '--doctype', metavar='<type>', default='_doc')
    parser.add_argument('-f', '--field', metavar='<field>', required=True)
    parser.add_argument('-o', '--output', metavar='<format>', default='json',
                        choices=['json', 'csv'])
    args = parser.parse_args()

    es = Elasticsearch(args.host, port=args.port)

    try:
        inv_index = get_inverted_index(es, args.index, args.doctype,
                                       args.field)
    except ElasticsearchException as e:
        print('An Elasticsearch error occurred:', file=sys.stderr)
        print(e, file=sys.stderr)
        exit(1)

    if args.output == 'csv':
        inv_index.write_csv(sys.stdout)
    elif args.output == 'json':
        inv_index.write_json(sys.stdout)


if __name__ == '__main__':
    main()
