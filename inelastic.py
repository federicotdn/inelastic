#!/usr/bin/env python3

import argparse
import sys
import json
from collections import defaultdict
from elasticsearch import Elasticsearch, ElasticsearchException
from tqdm import tqdm

SEARCH_SIZE = 200
SCROLL_TIME = '1m'
CSV_SEP = ','
CSV_EOL = '\n'


def vprint(*args, **kwargs):
    print(*args, **kwargs, file=sys.stderr)


class InvertedIndex:
    class IndexEntry:
        def __init__(self):
            self.ids = []
            self.freq = 0

    def __init__(self):
        self._max_uniq_freq = 0
        self._term_dict = defaultdict(InvertedIndex.IndexEntry)
        self._sorted_terms = []
        self._dirty = True

    def add_terms(self, doc_id, terms):
        for term, count in terms:
            entry = self._term_dict[term]
            entry.ids.append(doc_id)
            entry.freq += count
            self._max_uniq_freq = max(self._max_uniq_freq, len(entry.ids))

        self._dirty = True

    def _sort(self):
        if not self._dirty:
            return

        for entry in self._term_dict.values():
            entry.ids.sort()

        self._sorted_terms = sorted(self._term_dict.items(),
                                    key=lambda i: i[0])

        self._dirty = False

    def write_csv(self, fp):
        self._sort()

        fields = ['term', 'freq', 'freq_uniq']
        fields.extend('d{}'.format(i) for i in range(self._max_uniq_freq))
        fp.write('{}{}'.format(CSV_SEP.join(fields), CSV_EOL))

        for term, entry in self._sorted_terms:
            fp.write('{}{}'.format(term, CSV_SEP))
            fp.write('{}{}'.format(entry.freq, CSV_SEP))
            fp.write('{}{}'.format(len(entry.ids), CSV_SEP))
            fp.write('{}{}'.format(CSV_SEP.join(entry.ids), CSV_EOL))

    def write_json(self, fp):
        self._sort()

        obj = {
            'terms': [{
                'term': term,
                'freq_uniq': len(entry.ids),
                'freq': entry.freq,
                'ids': entry.ids
            } for term, entry in self._sorted_terms]
        }

        json.dump(obj, fp, indent=4, ensure_ascii=False)


def get_terms(terms_dict):
    for term, info in terms_dict.items():
        yield term, info['term_freq']


def get_inverted_index(es, index, doc_type, field, verbose):
    if verbose:
        doc_count = es.count(index=index)['count']
        vprint('Index: {}'.format(index))
        vprint('Document type: {}'.format(doc_type))
        vprint('Document field: {}'.format(field))
        vprint('Document count: {}'.format(doc_count))

    search = es.search(index=index, scroll=SCROLL_TIME, _source=False, body={
        'size': SEARCH_SIZE
    })

    errors = 0
    inv_index = InvertedIndex()
    if verbose:
        vprint('Reading term vectors...')
        pbar = tqdm(total=doc_count, file=sys.stderr)

    while search['hits']['hits']:
        hits = search['hits']['hits']
        ids = [hit['_id'] for hit in hits]

        resp = es.mtermvectors(ids=ids, index=index, doc_type=doc_type,
                               fields=field)

        for result in resp['docs']:
            doc_id = result['_id']
            if 'term_vectors' in result:
                terms = get_terms(result['term_vectors'][field]['terms'])
                inv_index.add_terms(doc_id, terms)
            else:
                errors += 1

        if verbose:
            pbar.update(len(ids))

        scroll_id = search['_scroll_id']
        search = es.scroll(scroll_id, scroll=SCROLL_TIME)

    if verbose:
        pbar.close()

    vprint('Done ({} mterm vectors errors).'.format(errors))

    es.clear_scroll(scroll_id)
    return inv_index


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--index', metavar='<name>', required=True,
                        help='Index name.')
    parser.add_argument('-e', '--host', metavar='<host>', default='localhost',
                        help='Elasticsearch host address.')
    parser.add_argument('-p', '--port', metavar='<port>', default=9200,
                        help='Elasticsearch host port.')
    parser.add_argument('-d', '--doctype', metavar='<type>', default='_doc',
                        help='Document type.')
    parser.add_argument('-f', '--field', metavar='<field>', required=True,
                        help='Document field.')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Enable verbose mode.')
    parser.add_argument('-o', '--output', metavar='<format>', default='json',
                        choices=['json', 'csv', 'null'],
                        help='Output format. Use \'null\' to omit output.')
    args = parser.parse_args()

    if not args.verbose:
        global vprint
        vprint = lambda *args, **kwargs: None  # noqa: E731

    es = Elasticsearch(args.host, port=args.port)

    vprint('Starting inelastic script...')

    try:
        inv_index = get_inverted_index(es, args.index, args.doctype,
                                       args.field, args.verbose)
    except ElasticsearchException as e:
        print('An Elasticsearch error occurred:', file=sys.stderr)
        print(e, file=sys.stderr)
        exit(1)

    vprint('Writing inverted index in {} format...'.format(args.output))

    if args.output == 'csv':
        inv_index.write_csv(sys.stdout)
    elif args.output == 'json':
        inv_index.write_json(sys.stdout)

    vprint('All done.')


if __name__ == '__main__':
    main()
