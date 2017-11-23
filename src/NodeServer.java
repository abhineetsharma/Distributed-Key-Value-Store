
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;

public class NodeServer {

    static int portNumber;
    static String nodeName;
    private static Map<String, Node.InitReplica.Replica> nodeMap;

    private static boolean printFlag = true;//flag to stop print

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
                        print(msg);
                        //Replica message from controller
                        if (msg.hasInitReplica()) {
                            print("----------InitReplica Start----------");
                            for(Node.InitReplica.Replica replica : msg.getInitReplica().getAllReplicaList()){
                                if(!replica.getName().equalsIgnoreCase(nodeName)){
                                    nodeMap.put(replica.getName(),replica);
                                }
                            }
                            print("----------InitReplica End----------");
                        }
                        //Read message from the controller
                        else if(msg.hasRead()){
                            print("----------Read Start----------");

                            print("----------Read End----------");
                        }
                        //Write message from the controller
                        else if(msg.hasWrite()){
                            print("----------Write Start----------");

                            print("----------Write End----------");
                        }
                        //Get Key message from coordinator
                        else if(msg.hasGetKey()){
                            print("----------GetKey Start----------");

                            print("----------GetKey End----------");
                        }
                        //Put Key message from coordinator
                        else if(msg.hasPutKeyVal()){
                            print("----------PutKeyVal Start----------");

                            print("----------PutKeyVal End----------");
                        }
                        //Read Repair Message from the coordinator
                        else if(msg.hasReadRepair()){
                            print("----------ReadRepair Start----------");

                            print("----------ReadRepair End----------");
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
    private static void print(Object obj) {
        if (printFlag) {
            if (obj != null)
                System.out.println(obj.toString());
            else
                System.out.println("null");
        }
    }


}