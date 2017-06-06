#!/usr/bin/env python3
import queue
import threading
import time
import random

num_worker_threads = 2

source = range(1, 100)

q = queue.Queue()
print_queue = queue.Queue()

threads = []
def do_work(item):
    print_queue.put(str.format("Item %s" % item))
    time.sleep(0.01 * random.randrange(1, 100))

def worker():
    while True:
        if q.empty():
            print_queue.put("Caught up, removing thread")
            break
        item = q.get(block=True)
        # Release the queue right after getting an item
        q.task_done()

        do_work(item)


def printer():
    while True:
        line = print_queue.get(block=True)
        if line is None:
            break
        print(line)
        print_queue.task_done()
print_thread = threading.Thread(target=printer)
#print_thread.daemon = True
print_thread.start()

for item in source:
    time.sleep(0.1)
    print("Putting item %s into the queue" % item)
    q.put(item)
    qsize = q.qsize()
    if qsize > 1:
        # Clean up threads
        threads = [t for t in threads if t.isAlive()]
        t = threading.Thread(target=worker)
        #t.daemon = True
        t.start()
        threads.append(t)
        print(
            "Lagging behind, %d items in the queue, started a thread. Current thread count: %d" % (qsize, len(threads))
        )

# block until all tasks are done
q.join()

# Close threads....
print_queue.put(None)
print_thread.join()
