
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;

public class NodeServer {

    static int portNumber;
    static String nodeName;
    private static Map<String, Node.InitReplica.Replica> nodeMap;

    static {
        nodeMap = new TreeMap<>();
    }

    public static void main(String[] args) {

        ServerSocket branchSocket = null;
        Socket clientSocket = null;

        try {
            if (args.length > 1) {
                nodeName = args[0];
                portNumber = Integer.parseInt(args[1]);

                branchSocket = new ServerSocket(portNumber);
                System.out.println("NodeServer Server Started");


                while (true) {
                    clientSocket = branchSocket.accept();
                    InputStream is = clientSocket.getInputStream();
                    Node.MapMessage msg = Node.MapMessage.parseDelimitedFrom(is);

                    if (msg != null) {
                        //Replica message from controller
                        if (msg.hasInitReplica()) {
                            for(Node.InitReplica.Replica replica : msg.getInitReplica().getAllReplicaList()){
                                if(!replica.getName().equalsIgnoreCase(nodeName)){
                                    nodeMap.put(replica.getName(),replica);
                                }
                            }
                        }
                        //Read message from the controller
                        else if(msg.hasRead()){

                        }
                        //Write message from the controller
                        else if(msg.hasWrite()){

                        }
                        //Get Key message from coordinator
                        else if(msg.hasGetKey()){

                        }
                        //Put Key message from coordinator
                        else if(msg.hasPutKeyVal()){

                        }
                        //Read Repair Message from the coordinator
                        else if(msg.hasReadRepair()){

                        }
                    }

                }
            }
        } catch (UnknownHostException e1) {
            e1.printStackTrace();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }


}