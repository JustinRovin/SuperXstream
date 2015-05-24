import Queue
import json
import subprocess as sp
import sys
import threading
import time

import blessings

term = blessings.Terminal()

colors = [
    term.blue,
    term.green,
    term.yellow,
    term.purple,
    term.red,
]

USAGE = '''%s <config-path> <graph-path>'''


def kill_all(processes):
    for proc in processes:
        proc.terminate()


def enqueue_output(out, queue, i):
    color = colors[i%len(colors)]
    for line in iter(out.readline, b''):
        queue.put(color(line.decode('utf-8').rstrip('\n')))
    out.close()


def print_queue(queue):
    while True:
        try:
            line = queue.get_nowait()
        except Queue.Empty:
            time.sleep(1)
        else:
            print line


def main(argv):
    if len(argv) != 3:
        print USAGE % argv[0]
        exit(1)

    config = json.load(open(argv[1], 'r'))

    processes = []
    # read_threads = []
    q = Queue.Queue()

    for i, host in enumerate(config['Hosts']):
        hostname, port = host.split(':')
        p = sp.Popen(['./bin/server', argv[1], port, argv[2]], stdout=sp.PIPE,
                stderr=sp.STDOUT, close_fds=True, bufsize=1)
        t = threading.Thread(target=enqueue_output, args=(p.stdout, q, i))
        t.daemon = True
        t.start()
        # read_threads.append(t)
        processes.append(p)

    printer = threading.Thread(target=print_queue, args=(q,))
    printer.daemon = True
    printer.start()

    try:
        while True:
            cmd = raw_input()
            if cmd == 'QUIT':
                kill_all(processes)
                exit(0)
    except KeyboardInterrupt:
        kill_all(processes)
        exit(0)


if __name__ == '__main__':
    main(sys.argv)

