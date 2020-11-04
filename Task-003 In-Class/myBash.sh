#!/bin/bash

x=$(jps)

echo "${x}"

if [[ $x == *"NodeManager"* ]];

	
then
	echo "Hadoop is active"

else 
	echo "Hadoop is inactive"

fi
