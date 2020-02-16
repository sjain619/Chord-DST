# cs457-557-f19-pa2-sjain619
cs457-557-f19-pa2-sjain619 created by GitHub Classroom
// Switch the remote server to bash mode
    bash
// Add the thrift compiler path to your PATH environment variable:

    export PATH=$PATH:/home/yaoliu/src_code/local/bin
	  export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:"/home/yaoliu/src_code/local/lib"

// Thrift command to compile thrift IDL file to Java

    thrift -gen java chord.thrift

// Steps to Compile and Run the project
1. Run make command to compile the code
    make
2. Run the server and provide the port no
   ./server.sh 8080
3. Run multiple servers and add ip and port of all the servers to node.txt
4. Run the initializer:
   chmod +x init
   ./init nodes.txt
5. Run the client and provide ip , port no, filename and read/write operation
   ./client.sh (ip) (port) (filename) (read/write)
