#!/usr/bin/env python3

from .utils import buffer_output, get_reduce_chunk_content

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

    word_counts = dict()
    for content in get_reduce_chunk_content(CHUNK_SIZE):
        cur_word_counts = reducer(content)
        for word, count in cur_word_counts.items():
            word_counts[word] = word_counts.get(word, 0) + count

    buffer_output(input_dict = word_counts, buffer_size = BUFFER_SIZE)

if __name__ == '__main__':
    main()