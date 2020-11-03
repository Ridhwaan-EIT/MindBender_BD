#!/usr/bin/env python
f = open('Shakespeare.txt', 'r')
z = [v for line in f for v in line.split()]
for x in z:
    x = x.isalnum()


def counter():
    frequency = dict()
    for v in z:
        if v in frequency:
            frequency[v] += 1
        else:
            frequency[v] = 1

    return frequency


print(counter())

hdfs_path = os.path.join(os.sep, fieldemployee, fieldemployee, Scala_Word_Counter.scala)


put = Popen(["hadoop", "fs", "-put", Scala_Word_Counter.scala, hdfs_path], stdin=PIPE, bufsize=-1)

put.communicate()
