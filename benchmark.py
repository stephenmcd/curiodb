#!/usr/bin/env python

# Benchmarks Redis and CurioDB, and writes the results to the README file.

from contextlib import contextmanager
from collections import OrderedDict
from subprocess import Popen, STDOUT, PIPE


def proc(cmd):
    return Popen(cmd, shell=True, stdout=PIPE, stderr=STDOUT)

def ping():
    while True:
        for line in proc("redis-cli PING").stdout:
            stripped = line.strip()
            if stripped:
                return stripped == "PONG"

def shutdown():
    proc("redis-cli shutdown")
    while ping():
        pass

def banner(msg):
    print "\n%s\n%s" % (msg, "-" * len(msg))

@contextmanager
def server(name):
    global which
    which = name
    shutdown()
    banner("Starting %s" % which)
    proc({"Redis": "redis-server", "CurioDB": "sbt run"}.get(which))
    while not ping():
        pass
    yield
    shutdown()

bench_count = 0
def benchmark(extra_text=""):
    global bench_count
    bench_count += 1
    if extra_text:
        extra_text += " "
    banner("Benchmarking %s %s%s/3" % (which, extra_text, bench_count))
    for line in proc("redis-benchmark -q").stdout:
        if line.strip():
            parts = line.split()
            name, requests = parts[0].rstrip(":"), float(parts[-4])
            data.setdefault(name, {})
            data[name][which] = requests
            print name.ljust(12), ("%.2f" % requests).rjust(10)


print "\nWelcome to benchmarks! This may take a couple of minutes..."

data = OrderedDict()

with server("Redis"):
    benchmark()

with server("CurioDB"):
    benchmark("(cold)")
    benchmark("(hot)")

with open("README.md") as f:
    lines = f.readlines()

before = lines[:lines.index("---------------|----------|----------|----\n") + 1]
after = lines[lines.index("## License\n") - 1:]
table = [("`%s`" % name).ljust(14)
         + " | " + ("%.2f" % result["Redis"]).rjust(8)
         + " | " + ("%.2f" % result["CurioDB"]).rjust(8)
         + " | %s%%\n" % int(result["CurioDB"] / result["Redis"] * 100)
         for name, result in data.items()]

with open("README.md", "w") as f:
    for line in before + table + after:
        f.write(line)
