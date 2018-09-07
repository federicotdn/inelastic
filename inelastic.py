import argparse
import sys
import json
import csv
from collections import defaultdict
from elasticsearch import Elasticsearch
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
        self._reset()

    def _reset(self):
        self._max_uniq_freq = 0
        self._term_dict = defaultdict(InvertedIndex.IndexEntry)
        self._sorted_terms = []
        self._dirty = True

    def read_index(self, es, index, doc_type, field, id_field=None):
        self._reset()
        search = es.search(
            index=index,
            scroll=SCROLL_TIME,
            _source=[id_field] if id_field else False,
            body={
                'size': SEARCH_SIZE
            }
        )
        scroll_id = None

        while search['hits']['hits']:
            hit_ids = {}
            for hit in search['hits']['hits']:
                val = hit['_source'][id_field] if id_field else hit['_id']
                hit_ids[hit['_id']] = val

            resp = es.mtermvectors(ids=list(hit_ids.keys()), index=index,
                                   doc_type=doc_type, fields=field)

            errors = 0
            for result in resp['docs']:
                doc_id = hit_ids[result['_id']]

                field_dict = result.get('term_vectors', {}).get(field, None)
                if field_dict:
                    terms = self._extract_terms(field_dict['terms'])
                    self._add_terms(doc_id, terms)
                else:
                    errors += 1

            yield len(hit_ids), errors

            scroll_id = search['_scroll_id']
            search = es.scroll(scroll_id, scroll=SCROLL_TIME)

        if scroll_id:
            es.clear_scroll(scroll_id)

    def _extract_terms(self, terms_dict):
        for term, info in terms_dict.items():
            yield term, info['term_freq']

    def _add_terms(self, doc_id, terms):
        for term, count in terms:
            entry = self._term_dict[term]
            entry.ids.append(doc_id)
            entry.freq += count
            self._max_uniq_freq = max(self._max_uniq_freq, len(entry.ids))

        self._dirty = True

    @property
    def term_count(self):
        return len(self._term_dict)

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

        writer = csv.writer(sys.stdout)
        fields = ['term', 'freq', 'doc_count']
        fields.extend('d{}'.format(i) for i in range(self._max_uniq_freq))

        writer.writerow(fields)

        for term, entry in self._sorted_terms:
            row = [term, entry.freq, len(entry.ids)]
            row.extend(entry.ids)
            writer.writerow(row)

    def write_json(self, fp):
        self._sort()

        obj = {
            'terms': [{
                'term': term,
                'doc_count': len(entry.ids),
                'freq': entry.freq,
                'ids': entry.ids
            } for term, entry in self._sorted_terms]
        }

        json.dump(obj, fp, indent=4, ensure_ascii=False)


def get_inverted_index(es, index, doc_type, field, id_field, verbose):
    if verbose:
        doc_count = es.count(index=index)['count']
        vprint('Index: {}'.format(index))
        vprint('Document type: {}'.format(doc_type))
        vprint('Document field: {}'.format(field))
        vprint('Document count: {}'.format(doc_count))

    errors = 0
    inv_index = InvertedIndex()
    if verbose:
        vprint('Reading term vectors...')
        pbar = tqdm(total=doc_count, file=sys.stderr)

    for n_docs, n_errs in inv_index.read_index(es, index, doc_type, field,
                                               id_field):
        if verbose:
            pbar.update(n_docs)
        errors += n_errs

    if verbose:
        pbar.close()

    vprint('Done ({} mterm vectors errors).'.format(errors))
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
    parser.add_argument(
        '-f', '--field', metavar='<field>', required=True,
        help='Document field from which to generate the inverted index.')
    parser.add_argument('-l', '--id-field', metavar='<ID field>',
                        help='Document field to use as ID.')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Enable verbose mode.')
    parser.add_argument('-o', '--output', metavar='<format>', default='csv',
                        choices=['json', 'csv', 'null'],
                        help='Output format. Use \'null\' to omit output.')
    args = parser.parse_args()

    if not args.verbose:
        global vprint
        vprint = lambda *args, **kwargs: None  # noqa: E731

    es = Elasticsearch(args.host, port=args.port)

    vprint('Starting inelastic script...')

    inv_index = get_inverted_index(es, args.index, args.doctype,
                                   args.field, args.id_field, args.verbose)

    if not inv_index.term_count:
        vprint('Error: Inverted index contains 0 terms.')
        exit(1)

    vprint('Writing inverted index in {} format...'.format(args.output))

    if args.output == 'csv':
        inv_index.write_csv(sys.stdout)
    elif args.output == 'json':
        inv_index.write_json(sys.stdout)

    vprint('All done.')


if __name__ == '__main__':
    main()
