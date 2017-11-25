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
            sendPUTrequestToCoordinator("node1");

//			Thread.sleep(10000);
//			sendGETrequestToCoordinator();
        }
    }

    private static void sendPUTrequestToCoordinator(String node) throws UnknownHostException, IOException {
        System.out.println("IP: " + nodeMap.get(node).getIp() + " Port:" + nodeMap.get(node).getPort());
        Socket socket = new Socket(nodeMap.get(node).getIp(), nodeMap.get(node).getPort());
        Node.ClientWriteRequest.Builder putKeyVal = Node.ClientWriteRequest.newBuilder();
        putKeyVal.setKey(1).setValue("XYZ").setConsistencyLevel(Node.ConsistencyLevel.TWO).build();
        Node.WrapperMessage.Builder msg = Node.WrapperMessage.newBuilder();
        msg.setClientWriteRequest(putKeyVal).build().writeDelimitedTo(socket.getOutputStream());
        socket.close();
    }

    private static void sendGETrequestToCoordinator(String node) throws UnknownHostException, IOException {
        System.out.println("IP: " + nodeMap.get(node).getIp() + " Port:" + nodeMap.get(node).getPort());
        Socket socket = new Socket(nodeMap.get(node).getIp(), nodeMap.get(node).getPort());
        Node.ClientReadRequest.Builder getKeyBuilder = Node.ClientReadRequest.newBuilder();
        getKeyBuilder.setKey(1).build();
        Node.WrapperMessage.Builder msg = Node.WrapperMessage.newBuilder();
        msg.setClientReadRequest(getKeyBuilder).build().writeDelimitedTo(socket.getOutputStream());
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
