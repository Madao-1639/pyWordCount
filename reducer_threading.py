#!/usr/bin/env python3

from threading import Thread
from .reducer import reducer
from .utils import buffer_output, get_reduce_chunk_content

def worker(content, word_counts):
    cur_word_counts = reducer(content)
    for word, count in cur_word_counts.items():
        word_counts[word] = word_counts.get(word, 0) + count

def main():
    CHUNK_SIZE = 1024 * 1024 * 100
    BUFFER_SIZE = 100

    word_counts = dict()
    threads = []
    for content in get_reduce_chunk_content(CHUNK_SIZE):
        thread = Thread(target = worker, args = (content, word_counts))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    buffer_output(input_dict = word_counts, buffer_size = BUFFER_SIZE)

if __name__ == '__main__':
    main()