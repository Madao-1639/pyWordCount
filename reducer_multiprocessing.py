#!/usr/bin/env python3

from multiprocessing import Pool
from .reducer import reducer
from .utils import buffer_output, get_reduce_chunk_content

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