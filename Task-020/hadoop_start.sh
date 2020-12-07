#!/bin/bash

output=$(jps)

if [[ "$output" == *"ResourceManager"* ]]; then
	echo Hadoop already running..!
else
	echo Hadoop not running..!
	eval start-all.sh
fi
