#!/usr/bin/env python3

import sys
from string import punctuation as punct
import re

SKIP_SIGNS = '\n \t' + punct
SKIP_REGEX = re.compile(r'[\s' + re.escape(punct) + ']')

def buffer_output(input_dict, buffer_size = 1000):
    output_buffer = [None] * buffer_size
    idx = 0
    for k,v in input_dict.items():
        output_buffer[idx] = '%s\t%s\n' %(k,v)
        idx += 1
        if idx == buffer_size:
            sys.stdout.write(''.join(output_buffer))
            sys.stdout.flush()
            idx = 0
    if idx > 0:
        sys.stdout.write(''.join(output_buffer[:idx]))
        sys.stdout.flush()
    
def get_map_chunk_content(chunk_size = 1024 * 1024 * 100):
    remain_str = ''
    while True:
        chunk = sys.stdin.read(chunk_size)
        if not chunk:
            break
        for idx in range(len(chunk) - 1, -1, -1):
            if chunk[idx] in SKIP_SIGNS:
                break
        if idx > -1:
            content = remain_str + chunk[:idx]
            yield content
        remain_str = chunk[idx + 1:]
    if remain_str:
        yield remain_str

def get_reduce_chunk_content(chunk_size = 1024 * 1024 * 100):
    remain_str = ''
    while True:
        chunk = sys.stdin.read(chunk_size)
        if not chunk:
            break
        idx = chunk.rfind('\n')
        if idx > -1:
            content = remain_str + chunk[:idx]
            yield content
        remain_str = chunk[idx + 1:]
    if remain_str:
        yield remain_str