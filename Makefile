LIB_PATH=/home/phao3/protobuf/protobuf-3.4.0/java/core/target/protobuf.jar

all: clean
	mkdir bin
	javac -classpath $(LIB_PATH) -d bin/ src/*.java

clean:
	rm -rf bin/
	rm -rf dir/

