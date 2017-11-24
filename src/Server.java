
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;

public class Server {

    static int currentServerPortNumber;
    static String currentServerName;
    static String filePath;
    static String currentServerIp;

    private static Map<String, NodeServerData> nodeMap;
    private static Map<String,ValueMetaData> keyValueMap;

    private static boolean printFlag = true;//flag to stop print

    static {
        nodeMap = new TreeMap<>();
    }

    public static void main(String[] args) {

        ServerSocket branchSocket = null;
        Socket clientSocket = null;


        try {
            if (args.length > 1) {

                currentServerPortNumber = Integer.parseInt(args[0]);
                filePath = args[1];

                branchSocket = new ServerSocket(currentServerPortNumber);
                System.out.println("Node Server Server Started");
                int msgCount = 0;

                //server information read by the server read by server
                FileProcessor fPro = new FileProcessor(filePath);
                String str = "";
                while ((str = fPro.readLine()) != null) {
                    NodeServerData nodeServerData = new NodeServerData(str);
                    nodeMap.put(nodeServerData.getName(), nodeServerData);

                    if (nodeServerData.getPort() == currentServerPortNumber) {
                        currentServerName = nodeServerData.getName();
                        currentServerIp = nodeServerData.getIp();
                    }
                }

                print(nodeMap);
                print(getReplicaServersList("node4"));

                while (true) {
                    clientSocket = branchSocket.accept();
                    InputStream is = clientSocket.getInputStream();
                    Node.WrapperMessage msg = Node.WrapperMessage.parseDelimitedFrom(is);


                    if (msg != null) {
                        print("\n\n----------------------------------------");
                        print("====Message received count = " + (++msgCount));
                        print("----------------------------------------\n\n");

                        print(msg);

                        //Read message from the controller
                        if (msg.hasClientReadRequest()) {
                            print("----------Client Read Request Start----------");

                            print("----------Client Read Request End----------");
                        }
                        //Write message from the controller
                        else if (msg.hasClientWriteRequest()) {
                            print("----------Client Write Request Start----------");

                            print("----------Client Write Request End----------");
                        }
                        //Get Key message from coordinator
                        else if (msg.hasGetKeyFromCoordinator()) {
                            print("----------Get Key From Coordinator Start----------");

                            print("----------Get Key From Coordinator End----------");
                        }
                        //Put Key message from coordinator
                        else if (msg.hasPutKeyFromCoordinator()) {
                            print("----------Put Key From Coordinator Start----------");

                            print("----------Put Key From Coordinator End----------");
                        }
                        //Read Repair Message from the coordinator
                        else if (msg.hasReadRepair()) {
                            print("----------ReadRepair Start----------");

                            print("----------ReadRepair End----------");
                        }

                        print("\n\n---------------------------------------------");
                        print("=====Message received End count = " + msgCount);
                        print("---------------------------------------------\n\n");
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

    private static void processRequest(String nodeName, String key, String value) {
        String dateStr = getCurrentTimeString();

        Thread thread = new Thread(() -> {
        });
        thread.start();
    }

    private static List<NodeServerData> getReplicaServersList(String keyNode) {
        String[] mapKeys = nodeMap.keySet().toArray(new String[nodeMap.size()]);
        int keyPosition = Arrays.asList(mapKeys).indexOf(keyNode);
        List<NodeServerData> nodeServerDataList = new ArrayList<>();

        for (int i = 0; i < 4 && i < nodeMap.size(); i++) {
            nodeServerDataList.add(nodeMap.get(mapKeys[(nodeMap.size() + keyPosition + i) % nodeMap.size()]));
        }

        return nodeServerDataList;
    }

    private static String getCurrentTimeString(){
        return Long.toString(System.currentTimeMillis());
    }


}