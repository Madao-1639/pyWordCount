#!/usr/bin/env python3

from multiprocessing import Pool
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

def reducer(content):
    word_counts = dict()
    cur_word = None
    cur_count = 0
    for line in content.split('\n'):
        if line == '':
            continue
        try:
            word, count = line.split('\t', 1)
            count = int(count)
        except ValueError:
            continue
        if cur_word == word:
            cur_count += count
        elif cur_word is not None:
            word_counts[cur_word] = cur_count
        cur_word = word
        cur_count = count
    if cur_word == word:
        word_counts[cur_word] = cur_count
    return word_counts

def main():
    CHUNK_SIZE = 1024 * 1024 * 100
    BUFFER_SIZE = 100
    MAX_NUM_PROCESS = 2

    word_counts = dict()
    with Pool(processes=MAX_NUM_PROCESS) as pool:
        for cur_word_counts in pool.imap_unordered(reducer, get_reduce_chunk_content(CHUNK_SIZE)):
            for word, count in cur_word_counts.items():
                word_counts[word] = word_counts.get(word, 0) + count

    buffer_output(input_dict = word_counts, buffer_size = BUFFER_SIZE)

if __name__ == '__main__':
    main()