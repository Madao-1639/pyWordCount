#!/usr/bin/env python3

from multiprocessing import Pool
from .mapper import mapper
from .utils import buffer_output, get_map_chunk_content

def main():
    BUFFER_SIZE = 1000
    CHUNK_SIZE = 1024 * 1024 * 200
    MAX_NUM_PROCESS = 2

    with Pool(processes=MAX_NUM_PROCESS) as pool:
        for word_counts in pool.imap_unordered(mapper, get_map_chunk_content(CHUNK_SIZE)):
            buffer_output(input_dict = word_counts, buffer_size= BUFFER_SIZE)

if __name__ == '__main__':
    main()
