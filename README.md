# inelastic
Print an Elasticsearch inverted index as a table (CSV) or JSON object.

## Usage

```bash
usage: inelastic.py [-h] -i <name> [-e <host>] [-p <port>] [-d <type>] -f <field>
                    [-o <format>]

optional arguments:
  -h, --help            show this help message and exit
  -i <name>, --index <name>
  -e <host>, --host <host>
  -p <port>, --port <port>
  -d <type>, --doctype <type>
  -f <field>, --field <field>
  -o <format>, --output <format>
```

## Examples

```bash
$ ./inelastic.py -f content -i tweets -o csv > tweets.csv
$ ./inelastic.py -f description -i products -o json > products.json
```
