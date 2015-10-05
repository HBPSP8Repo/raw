import csv

def parse_csv(content, properties):
    sniffer = csv.Sniffer()
    dialect = sniffer.sniff(content, delimiters=";,|")
    content = content.splitlines()
    if properties['has_header']:
        content = content[1:]
    reader = csv.reader(content, dialect)
    # msb: Unfortunately can't return an iterator since our code generated library doesn't support it...
    #for row in reader:
    #    yield {properties['field_names'][i]: v for i, v in enumerate(row)}
    rows = []
    for row in reader:
        rows.append({properties['field_names'][i]: v for i, v in enumerate(row)})
    return rows
