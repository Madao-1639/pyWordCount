#!/usr/bin/env python3

from .utils import buffer_output, get_map_chunk_content, SKIP_REGEX

def mapper(content):
    word_counts = dict()
    for word in SKIP_REGEX.split(content):
        if word == '':
            continue
        word = word.lower()
        word_counts[word] = word_counts.get(word,0) + 1
    return word_counts

def main():
    CHUNK_SIZE = 1024 * 1024 * 200
    BUFFER_SIZE = 1000
    for content in get_map_chunk_content(CHUNK_SIZE):
        buffer_output(input_dict = mapper(content), buffer_size = BUFFER_SIZE)

if __name__ == '__main__':
    main()
