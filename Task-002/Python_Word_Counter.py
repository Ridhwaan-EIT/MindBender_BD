#!/usr/bin/env python
import string 
import re
import json 
import pydoop.hdfs as hdfs 

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


def send_file(file):
    print("Saving to HDFS")

    dest = 'hdfs://localhost:9000/Task-002/python_output.txt'
    hdfs.put(file, dest)
    print("Saved to HDFS")

def save_output(dic):
    dumps = json.dumps(dic, sort_keys = True, indent=4)
    with open('python_output.txt', "w") as file:
        file.write(dumps)
        

def main():
    save_output(counter())
    send_file("python_output.txt")
    print("Completed")
    print(file)
