import json
import xml.etree.ElementTree as ET

def decode(body, headers):
    return [{'key': str(x.get('id',x['kind'])), 'value': x} for line in body.decode('utf-8').split(chr(30)) if line.strip() for x in [json.loads(line.strip())]]
