#!/usr/bin/env python3
"""Inspect the bundled API offline, without loading the entire document into chat."""
import argparse
import json
from pathlib import Path

SPEC = Path(__file__).resolve().parents[1] / 'references/openapi.json'

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    search = sub.add_parser('search')
    search.add_argument('term')
    operation = sub.add_parser('operation')
    operation.add_argument('method')
    operation.add_argument('path')
    args = parser.parse_args()
    schema = json.loads(SPEC.read_text())
    if args.command == 'search':
        for path, methods in schema['paths'].items():
            for method, op in methods.items():
                if args.term.lower() in (path + ' ' + op.get('summary','') + ' ' + ' '.join(op.get('tags',[]))).lower():
                    print(f"{method.upper():6} {path} — {op.get('summary','')}")
        return
    op = schema['paths'].get(args.path, {}).get(args.method.lower())
    if op is None:
        parser.error('Operation is not present in this contract')
    components = {}
    def collect(value):
        if isinstance(value,dict):
            ref = value.get('$ref','')
            if ref.startswith('#/components/') and ref not in components:
                node = schema
                for part in ref[2:].split('/'):
                    node = node[part]
                components[ref] = node
                collect(node)
            for item in value.values():
                collect(item)
        elif isinstance(value,list):
            for item in value:
                collect(item)
    collect(op)
    print(json.dumps({'operation':op,'referenced_components':components},ensure_ascii=False,indent=2))

if __name__ == '__main__':
    main()
