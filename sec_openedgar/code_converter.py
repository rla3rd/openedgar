#!/usr/bin/env python
"""
MIT License
Copyright (c) 2024 Richard Albright
Copyright (c) 2018 ContraxSuite, LLC

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

"""
Code Converter Tool for Django ORM to SQLAlchemy Migration

This script helps convert Django ORM code to SQLAlchemy by:
1. Scanning Python files for Django ORM patterns
2. Suggesting SQLAlchemy replacements
3. Optionally applying the changes

Usage:
    python code_converter.py scan <directory> - Scan for Django ORM patterns
    python code_converter.py convert <file> - Convert a specific file
"""

import os
import sys
import re
import argparse
from pathlib import Path

# Django ORM patterns to look for
DJANGO_PATTERNS = {
    r'([A-Za-z0-9_]+)\.objects\.all\(\)': {
        'description': 'Query all objects',
        'replacement': 'session.query({0}).all()'
    },
    r'([A-Za-z0-9_]+)\.objects\.filter\(([^)]+)\)': {
        'description': 'Filter objects',
        'replacement': 'session.query({0}).filter({0}.{1})'
    },
    r'([A-Za-z0-9_]+)\.objects\.get\(([^)]+)\)': {
        'description': 'Get a single object',
        'replacement': 'session.query({0}).filter({0}.{1}).first()'
    },
    r'([A-Za-z0-9_]+)\.objects\.create\(([^)]+)\)': {
        'description': 'Create an object',
        'replacement': '{0}({1})'
    },
    r'([A-Za-z0-9_]+)\.save\(\)': {
        'description': 'Save an object',
        'replacement': 'session.add({0})\nsession.commit()'
    },
    r'([A-Za-z0-9_]+)\.delete\(\)': {
        'description': 'Delete an object',
        'replacement': 'session.delete({0})\nsession.commit()'
    },
    r'([A-Za-z0-9_]+)\.objects\.order_by\(([^)]+)\)': {
        'description': 'Order objects',
        'replacement': 'session.query({0}).order_by({0}.{1})'
    },
    r'([A-Za-z0-9_]+)\.objects\.values\(([^)]+)\)': {
        'description': 'Get values',
        'replacement': 'session.query({0}.{1})'
    },
    r'([A-Za-z0-9_]+)\.objects\.values_list\(([^)]+)\)': {
        'description': 'Get values list',
        'replacement': 'session.query({0}.{1})'
    },
    r'([A-Za-z0-9_]+)\.objects\.count\(\)': {
        'description': 'Count objects',
        'replacement': 'session.query({0}).count()'
    },
    r'([A-Za-z0-9_]+)\.objects\.exists\(\)': {
        'description': 'Check if objects exist',
        'replacement': 'session.query({0}).exists()'
    },
    r'([A-Za-z0-9_]+)\.objects\.bulk_create\(([^)]+)\)': {
        'description': 'Bulk create objects',
        'replacement': 'session.add_all({1})\nsession.commit()'
    },
    r'([A-Za-z0-9_]+)\.objects\.update\(([^)]+)\)': {
        'description': 'Update objects',
        'replacement': 'session.query({0}).update({{{1}}})\nsession.commit()'
    },
    r'([A-Za-z0-9_]+)\.objects\.get_or_create\(([^)]+)\)': {
        'description': 'Get or create an object',
        'replacement': 'get_or_create(session, {0}, {1})'
    },
    r'([A-Za-z0-9_]+)\.objects\.update_or_create\(([^)]+)\)': {
        'description': 'Update or create an object',
        'replacement': 'update_or_create(session, {0}, {1})'
    },
}

# Helper function patterns to add
HELPER_IMPORTS = [
    'from openedgar.db import Session',
    'from openedgar.db.helpers import session_scope, get_or_create, update_or_create, filter_queryset',
]


def scan_file(file_path):
    """Scan a file for Django ORM patterns."""
    with open(file_path, 'r') as f:
        content = f.read()
    
    matches = []
    for pattern, info in DJANGO_PATTERNS.items():
        for match in re.finditer(pattern, content):
            matches.append({
                'pattern': pattern,
                'match': match.group(0),
                'line_num': content[:match.start()].count('\n') + 1,
                'description': info['description'],
                'replacement': info['replacement'].format(*match.groups())
            })
    
    return matches


def scan_directory(directory):
    """Scan a directory for Django ORM patterns in Python files."""
    results = {}
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)
                matches = scan_file(file_path)
                if matches:
                    results[file_path] = matches
    
    return results


def print_scan_results(results):
    """Print the results of a scan."""
    total_matches = sum(len(matches) for matches in results.values())
    print(f"Found {total_matches} Django ORM patterns in {len(results)} files.\n")
    
    for file_path, matches in results.items():
        print(f"File: {file_path}")
        print(f"Found {len(matches)} matches:")
        
        for match in matches:
            print(f"  Line {match['line_num']}: {match['description']}")
            print(f"    Django: {match['match']}")
            print(f"    SQLAlchemy: {match['replacement']}\n")


def convert_file(file_path):
    """Convert Django ORM patterns to SQLAlchemy in a file."""
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Check if we need to add helper imports
    needs_helpers = False
    for pattern in DJANGO_PATTERNS.values():
        if 'get_or_create' in pattern['replacement'] or 'update_or_create' in pattern['replacement']:
            if re.search(pattern['replacement'], content):
                needs_helpers = True
                break
    
    # Add session context manager
    if 'session.' in content and 'session_scope' not in content:
        content = re.sub(
            r'def ([^\(]+)\(([^\)]*)\):\s*',
            r'def \1(\2):\n    with session_scope() as session:\n        ',
            content
        )
        needs_helpers = True
    
    # Add imports if needed
    if needs_helpers and not any(imp in content for imp in HELPER_IMPORTS):
        import_pos = content.find('import')
        if import_pos >= 0:
            # Find the end of the import block
            lines = content.split('\n')
            import_end = 0
            in_import_block = False
            
            for i, line in enumerate(lines):
                if line.startswith('import ') or line.startswith('from '):
                    in_import_block = True
                elif in_import_block and not line.strip():
                    import_end = i
                    break
            
            if import_end > 0:
                for imp in HELPER_IMPORTS:
                    lines.insert(import_end, imp)
                content = '\n'.join(lines)
    
    # Replace Django ORM patterns with SQLAlchemy
    for pattern, info in DJANGO_PATTERNS.items():
        def replace_match(match):
            return info['replacement'].format(*match.groups())
        
        content = re.sub(pattern, replace_match, content)
    
    # Write the converted content back to the file
    backup_path = file_path + '.bak'
    os.rename(file_path, backup_path)
    
    with open(file_path, 'w') as f:
        f.write(content)
    
    print(f"Converted {file_path}")
    print(f"Backup saved to {backup_path}")


def main():
    parser = argparse.ArgumentParser(description='Django ORM to SQLAlchemy Code Converter')
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Scan command
    scan_parser = subparsers.add_parser('scan', help='Scan for Django ORM patterns')
    scan_parser.add_argument('directory', help='Directory to scan')
    
    # Convert command
    convert_parser = subparsers.add_parser('convert', help='Convert Django ORM to SQLAlchemy')
    convert_parser.add_argument('file', help='File to convert')
    
    args = parser.parse_args()
    
    if args.command == 'scan':
        results = scan_directory(args.directory)
        print_scan_results(results)
    elif args.command == 'convert':
        convert_file(args.file)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
