#!/bin/bash +vx
LIB_PATH=$"/home/phao3/protobuf/protobuf-3.4.0/java/core/target/protobuf.jar"

java -classpath bin:$LIB_PATH: Server $1 $2 $3
