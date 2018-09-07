# inelastic
Print an Elasticsearch inverted index as a CSV table or JSON object.


`inelastic` builds an approximation of how an inverted index would look like for a particular index and document field, using the [Multi termvectors API](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-multi-termvectors.html) on all stored documents.

## Example

Having the following index:
```
PUT /tweets
{
    "mappings": {
        "_doc": {
            "properties": {
                "content": {
                    "type": "text"
                }
            }
        }
    }
}
```

with the following documents:
```
POST /tweets/_doc/_bulk
{ "index": { "_id": 1 }}
{ "content": "This is my first tweet." }
{ "index": { "_id": 2 }}
{ "content": "Most Elasticsearch examples use tweets." }
{ "index": { "_id": 3 }}
{ "content": "This is an example." }
{ "index": { "_id": 4 }}
{ "content": "Adding some more tweets." }
{ "index": { "_id": 5 }}
{ "content": "Adding more and more tweets." }
```

`inelastic` could be used as follows (combined with the `column` command):

```bash
$ ./inelastic.py -i tweets -f content | column -t -s ,
```

Which would output:
```
term           freq  doc_count  d0  d1  d2
adding         2     2          4   5
an             1     1          3
and            1     1          5
elasticsearch  1     1          2
example        1     1          3
examples       1     1          2
first          1     1          1
is             2     2          1   3
more           3     2          4   5
most           1     1          2
my             1     1          1
some           1     1          4
this           2     2          1   3
tweet          1     1          1
tweets         3     3          2   4   5
use            1     1          2
```

The `freq` field specifies the total amount of times the term appears in all documents, and the `doc_count` field specifies how many documents contain the term at least once. The `d0`, `d1`... fields list the IDs for documents containing the term.

The chosen document field's type must be `text` or `keyword`.

## Installation
Having `git` and Python 3 installed, use:
```bash
$ git clone https://github.com/federicotdn/inelastic.git
$ cd inelastic
$ pip3 install -r requirements.txt # Optionally, do this inside a virtual environment
$ chmod +x inelastic.py
```

Inelastic is compatible with Elasticsearch version `6.0` and later.

## Usage
These are the arguments `inelastic` accepts:
- `-i` (`--index`): Index name (**required**).
- `-f` (`--field`): Document field name from which to generate inverted index (**required**).
- `-l` (`--id-field`): Document field to use as ID when printing results (*default: _id*).
- `-o` (`--output`): Output format, `json` or `csv` (*default: `csv`*).
- `-p` (`--port`): Elasticsearch host port (*default: 9200*).
- `-e` (`--host`): Elasticsearch host address (*default: localhost*).
- `-d` (`--doctype`): Document type (*default: _doc*).
- `-v` (`--verbose`): Print debug information (*default: false*).
