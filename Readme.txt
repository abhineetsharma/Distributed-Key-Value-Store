Prog. Lang used : Java.

Compile:
make all    	: Will compile all java classes.
make clean  	: To clean compiled files as well as log files directory.


Run:
For Server :  	       ./server <Port : 9090> Node.txt <Readrepair_Mode>
For Client :	       ./client Node.txt <clientNumber>

Implementation:
We have implemented given assingment in Java with the help of Google protocol serialisation.
When a client sends a request to any co-ordinator, co-ordinator will process the key provided and look
for the corresponding replica's(we used a modulo function to determine primary replica)
Co-ordinator sends requests in parallel way to all replicas, and then replica process those requests.
As soon as required consistency level fulfilled co-ordinator respond to client with latest value.
If there's need of readRepair co-ordinator will do that in background thread.
If there's any replica down for write request, co-ordinator will store hint and start hinted Handoff
when replica which was down becomes up and send msg to (previous) co-ordinator.
3rd Parameter passed to server should be the read repair or Hinted Handoff configuration.
1:Read Repair ON
2:Hinted HandOff ON.

#Please Note following:
1. Please put Node.txt in the same directory as of server n client script.
2. We have used the writeDelimitedTo method for google prot serialization. Hence I have used parseDelimitedFrom to de-serialize it.
3. Please delete "dir" directory which contain log files before every fresh run of servers.[Use make clean]
4. Please put Google ProtoBuff generated file (myCassandra.java) inside the src folder.
5. Please execute following programs before executing
     export PATH=/home/phao3/protobuf/bin/bin:$PATH
     protoc --proto_path=./ --java_out=src/ myCassandra.proto


Sample Output 


