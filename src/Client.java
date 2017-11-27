import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;

public class Client {
    private static String filePath;
    private static Map<String, NodeServerData> nodeMap;
    private static boolean printFlag = true;// flag to stop print

    static {
        nodeMap = new TreeMap<>();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        ServerSocket branchSocket = null;
        Socket clientSocket = null;
        if (args.length == 1) {
            filePath = args[0];
            System.out.println("Client Started");
            FileProcessor fPro = new FileProcessor(filePath);
            String str = "";
            while ((str = fPro.readLine()) != null) {
                NodeServerData nodeServerData = new NodeServerData(str);
                nodeMap.put(nodeServerData.getName(), nodeServerData);
            }
            print(nodeMap);


            for (int i = 0; i < 256; i++) {
                int no = (i + nodeMap.size()) % nodeMap.size();
                sendPUTRequestToCoordinator("node" + no, i, "XYZO"+i, Node.ConsistencyLevel.TWO);

                //Thread.sleep(5000);
                sendGETRequestToCoordinator("node" + no, i, Node.ConsistencyLevel.ONE);

                sendGETRequestToCoordinator("node5", i, Node.ConsistencyLevel.TWO);
            }

        }
    }

    private static void sendPUTRequestToCoordinator(String node, int key, String value, Node.ConsistencyLevel consistencyLevel) throws UnknownHostException, IOException {
        System.out.println("IP: " + nodeMap.get(node).getIp() + " Port:" + nodeMap.get(node).getPort());
        Socket socket = new Socket(nodeMap.get(node).getIp(), nodeMap.get(node).getPort());
        Node.ClientWriteRequest.Builder putKeyVal = Node.ClientWriteRequest.newBuilder();
        putKeyVal.setKey(key).setValue(value).setConsistencyLevel(consistencyLevel).build();
        Node.WrapperMessage.Builder msg = Node.WrapperMessage.newBuilder();
        msg.setClientWriteRequest(putKeyVal).build().writeDelimitedTo(socket.getOutputStream());


        Node.WrapperMessage message = Node.WrapperMessage.parseDelimitedFrom(socket.getInputStream());
        System.out.println("Message Received : " + message);
        socket.close();
    }

    private static void sendGETRequestToCoordinator(String node, int key, Node.ConsistencyLevel consistencyLevel) throws IOException {
        System.out.println("IP: " + nodeMap.get(node).getIp() + " Port:" + nodeMap.get(node).getPort());
        Socket socket = new Socket(nodeMap.get(node).getIp(), nodeMap.get(node).getPort());
        Node.ClientReadRequest.Builder getKeyVal = Node.ClientReadRequest.newBuilder();
        getKeyVal.setKey(key).setConsistencyLevel(consistencyLevel).build();
        Node.WrapperMessage.Builder msg = Node.WrapperMessage.newBuilder();
        msg.setClientReadRequest(getKeyVal).build().writeDelimitedTo(socket.getOutputStream());


        Node.WrapperMessage message = Node.WrapperMessage.parseDelimitedFrom(socket.getInputStream());
        System.out.println("Message Received : " + message);
        socket.close();
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
