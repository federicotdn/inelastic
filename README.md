# inelastic
Print an Elasticsearch inverted index as a table (CSV) or JSON object.

## Usage

```bash
usage: inelastic.py [-h] -i <name> [-e <host>] [-p <port>] [-d <type>] -f <field> [-v] [-o <format>]

optional arguments:
  -h, --help            show this help message and exit
  -i <name>, --index <name>
                        Index name.
  -e <host>, --host <host>
                        Elasticsearch host address.
  -p <port>, --port <port>
                        Elasticsearch host port.
  -d <type>, --doctype <type>
                        Document type.
  -f <field>, --field <field>
                        Document field.
  -v, --verbose         Enable verbose mode.
  -o <format>, --output <format>
                        Output format. Use 'null' to omit output.
```

## Examples

```bash
$ ./inelastic.py -f content -i tweets -o csv > tweets.csv
$ ./inelastic.py -f description -i products -o json > products.json
```

## Todo
- Add better examples
- Add more term statistics
- Handle same term appearing multiple times in the same doc
