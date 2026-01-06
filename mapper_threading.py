#!/usr/bin/env python3

from threading import Thread
from .mapper import mapper
from .utils import buffer_output, get_map_chunk_content

def worker(content, buffer_size):
    buffer_output(input_dict = mapper(content), buffer_size = buffer_size)

def main():
    CHUNK_SIZE = 1024 * 1024 * 100
    BUFFER_SIZE = 100

    threads = []
    for content in get_map_chunk_content(CHUNK_SIZE):
        thread = Thread(target = worker, args = (content, BUFFER_SIZE))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

if __name__ == '__main__':
    main()
