LIB_PATH=/home/yaoliu/src_code/local/lib/usr/local/lib/libthrift-0.12.0.jar:/home/yaoliu/src_code/local/lib/usr/local/lib/slf4j-log4j12-1.7.12.jar:/home/yaoliu/src_code/local/lib/usr/local/lib/slf4j-api-1.7.12.jar
all: clean
	mkdir bin
	mkdir bin/client_classes
	mkdir bin/server_classes
	javac -classpath $(LIB_PATH) -d bin/client_classes/ src/Client.java src/FileStoreHandler.java gen-java/*
	javac -classpath $(LIB_PATH) -d bin/server_classes/ src/MyServer.java src/FileStoreHandler.java gen-java/*


clean:
	rm -rf bin/

