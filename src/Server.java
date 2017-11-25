
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class Server {

    private int portNumber;
    private String name;
    private String filePath;
    private String ip;
    private Map<String, NodeServerData> nodeMap;
    private Map<Integer, ValueMetaData> keyValueMap;
    private String logFileName;
    private static boolean printFlag = true;// flag to stop print

    private void initServer() {
        // server information read by the server read by server
        FileProcessor fPro = new FileProcessor(filePath);
        nodeMap = new TreeMap<>();
        keyValueMap = new ConcurrentSkipListMap<>();
        String str = "";
        while ((str = fPro.readLine()) != null) {
            NodeServerData nodeServerData = new NodeServerData(str);
            nodeMap.put(nodeServerData.getName(), nodeServerData);

            if (nodeServerData.getPort() == portNumber) {
                name = nodeServerData.getName();
                ip = nodeServerData.getIp();
            }
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

    private void processRequest(String nodeName, String key, String value) {
        String dateStr = getCurrentTimeString();

        Thread thread = new Thread(() -> {
        });
        thread.start();
    }

    private List<NodeServerData> getReplicaServersList(String keyNode) {
        String[] mapKeys = this.nodeMap.keySet().toArray(new String[nodeMap.size()]);
        int keyPosition = Arrays.asList(mapKeys).indexOf(keyNode);
        List<NodeServerData> nodeServerDataList = new ArrayList<>();

        for (int i = 0; i < 4 && i < nodeMap.size(); i++) {
            nodeServerDataList.add(nodeMap.get(mapKeys[(nodeMap.size() + keyPosition + i) % nodeMap.size()]));
        }

        return nodeServerDataList;
    }

    private NodeServerData getNodeByKey(int key) {
        int num = (key + nodeMap.size()) % nodeMap.size();
        String[] nodeKeys = nodeMap.keySet().toArray(new String[nodeMap.size()]);
        return nodeMap.get(nodeKeys[num]);
    }

    private void processingClientWriteRequest(Node.ClientWriteRequest clientWriteRequest) {
        int key = clientWriteRequest.getKey();
        String value = clientWriteRequest.getValue();
        NodeServerData primaryReplica = getNodeByKey(key);
        print(primaryReplica.toString());
        List<NodeServerData> replicaServerList = getReplicaServersList(primaryReplica.getName());
        print(replicaServerList);

        String timeStamp = getCurrentTimeString();

        for (NodeServerData replica : replicaServerList) {

            Node.PutKeyFromCoordinator.Builder putKeyFromCoordinatorBuilder = Node.PutKeyFromCoordinator.newBuilder();

            putKeyFromCoordinatorBuilder.setKey(key);
            putKeyFromCoordinatorBuilder.setTimeStamp(timeStamp);
            putKeyFromCoordinatorBuilder.setValue(value);
            putKeyFromCoordinatorBuilder.setCoordinatorName(name);

            Node.WrapperMessage message = Node.WrapperMessage.newBuilder()
                    .setPutKeyFromCoordinator(putKeyFromCoordinatorBuilder).build();
            try {
                sendMessageToNodeSocket(replica, message);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void processingPutKeyFromCoordinatorRequest(Node.PutKeyFromCoordinator putKeyFromCoordinator)
            throws IOException {
        int key = putKeyFromCoordinator.getKey();
        String value = putKeyFromCoordinator.getValue();
        String timeStamp = putKeyFromCoordinator.getTimeStamp();
        String coordinatorName = putKeyFromCoordinator.getCoordinatorName();

        // BEGINE : Nikhil's pre commite code
        // Writting to log file.
        StringBuilder builder = new StringBuilder();
        this.logFileName = builder.append(this.name).append("LogFile").toString();

        Node.LogMessage logMessage = Node.LogMessage.newBuilder().setKey(putKeyFromCoordinator.getKey())
                .setValue(putKeyFromCoordinator.getValue()).setTimeStamp(putKeyFromCoordinator.getTimeStamp())
                .setLogEndFlag(false).build();
        Node.LogBook.Builder logBook = Node.LogBook.newBuilder();

        // Read the existing Log book.
        try {
            logBook.mergeFrom(new FileInputStream(logFileName));
        } catch (FileNotFoundException e) {
            System.out.println(": File not found.  Creating a new file.");
        } catch (IOException ex) {
            System.out.println("Error reading log file");
            ex.printStackTrace();
        }

        // Add a logMessage.
        logBook.addLog(logMessage);

        // Write the new log book back to disk.
        FileOutputStream output;
        try {
            output = new FileOutputStream(logFileName);
            logBook.build().writeTo(output);
            output.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Read the existing Log book.
        logBook = Node.LogBook.newBuilder();
        try {
            logBook.mergeFrom(new FileInputStream(logFileName));
        } catch (FileNotFoundException e) {
            System.out.println(": File not found.  Creating a new file.");
        } catch (IOException ex) {
            System.out.println("Error reading log file");
            ex.printStackTrace();
        }

        // END: Nikhil's pre commite code
        // DONE : do pre-commit processing via log file and code

        Node.LogBook.Builder newLogBook = Node.LogBook.newBuilder();
        for (Node.LogMessage log : logBook.getLogList()) {
            if (log.getLogEndFlag()) {
                newLogBook.addLog(log);
            } else {

                if (keyValueMap.containsKey(key)) {
                    keyValueMap.get(key).updateKeyWithValue(timeStamp, value);
                } else {
                    // this.dataStore.put(log.getKey(), log.getValue());

                    keyValueMap.put(key, new ValueMetaData(timeStamp, value));
                }
                // do post commit processing via log file and code
                // update log message and add to newLogBook
                Node.LogMessage newLogMessage = Node.LogMessage.newBuilder().setKey(log.getKey())
                        .setValue(log.getValue()).setTimeStamp(log.getTimeStamp()).setLogEndFlag(true).build();
                newLogBook.addLog(newLogMessage);

                print(keyValueMap.get(key));
                // Create acknowledgement and message to coordinator

                Node.AcknowledgementToCoordinator.Builder acknowledgementBuilder = Node.AcknowledgementToCoordinator
                        .newBuilder();
                acknowledgementBuilder.setKey(key);
                acknowledgementBuilder.setTimeStamp(timeStamp);
                acknowledgementBuilder.setValue(value);
                acknowledgementBuilder.setReplicaName(name);

                Node.WrapperMessage message = Node.WrapperMessage.newBuilder()
                        .setAcknowledgementToCoordinator(acknowledgementBuilder).build();

                NodeServerData coordinator = nodeMap.get(coordinatorName);

                // send acknowledgement to coordinator
                sendMessageToNodeSocket(coordinator, message);
                System.out.println("Ack message send to Co-ordinator : " + coordinatorName + ".....");
            }

            // Write updated logbook back to disk.
            try {
                output = new FileOutputStream(logFileName);
                newLogBook.build().writeTo(output);
                output.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    private void sendMessageToNodeSocket(NodeServerData nodeServerData, Node.WrapperMessage message)
            throws IOException {
        Socket nodeServerSocket = null;

        nodeServerSocket = new Socket(nodeServerData.getIp(), nodeServerData.getPort());
        message.writeDelimitedTo(nodeServerSocket.getOutputStream());
        nodeServerSocket.close();
    }

    public static void main(String[] args) {

        Server server = new Server();

        if (args.length > 1) {
            server.portNumber = Integer.parseInt(args[0]);
            server.filePath = args[1];

        } else {
            System.out.println("Invalid number of arguments to controller");
            System.exit(0);
        }

        server.initServer();

        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(server.portNumber);
            System.out.println("Server started with " + server.portNumber);
        } catch (IOException ex) {
            System.out.println("Server socket cannot be created");
            ex.printStackTrace();
            System.exit(1);
        }
        int msgCount = 0;
        while (true) {
            Socket receiver = null;
            try {
                receiver = serverSocket.accept();
                print("\n\n----------------------------------------");
                print("====Message received count = " + (++msgCount));
                print("----------------------------------------\n\n");
                Node.WrapperMessage message = Node.WrapperMessage.parseDelimitedFrom(receiver.getInputStream());

                print(message);
                if (message != null) {
                    if (message.hasClientReadRequest()) {
                        print("----ClientReadRequest Start----");
                        // call appropriate method from here
                        receiver.close();
                        print("----ClientReadRequest End----");
                    } else if (message.hasClientWriteRequest()) {
                        print("----ClientWriteRequest Start----");
                        // call appropriate method from here
                        server.processingClientWriteRequest(message.getClientWriteRequest());
                        receiver.close();
                        print("----ClientWriteRequest End----");
                    } else if (message.hasGetKeyFromCoordinator()) {
                        print("----GetKeyFromCoordinator Start----");
                        // call appropriate method from here
                        receiver.close();
                        print("----GetKeyFromCoordinator End----");
                    } else if (message.hasPutKeyFromCoordinator()) {
                        print("----PutKeyFromCoordinator Start----");
                        // call appropriate method from here
                        server.processingPutKeyFromCoordinatorRequest(message.getPutKeyFromCoordinator());
                        receiver.close();
                        print("----PutKeyFromCoordinator End----");
                    } else if (message.hasReadRepair()) {
                        print("----ReadRepair Start----");
                        // call appropriate method from here
                        receiver.close();
                        print("----ReadRepair End----");
                    }
                }
            } catch (IOException e) {
                System.out.println("Error reading data from socket. Exiting main thread");
                e.printStackTrace();
                System.exit(1);
            } finally {
                if (receiver != null) {
                    try {
                        receiver.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            print("\n\n---------------------------------------------");
            print("=====Message received End count = " + msgCount);
            print("---------------------------------------------\n\n");
        }
    }

    private String getCurrentTimeString() {
        return Long.toString(System.currentTimeMillis());
    }

}