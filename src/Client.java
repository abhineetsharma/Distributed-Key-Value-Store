import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.TreeMap;

public class Client {
    private static String filePath;
    private static Map<String, ServerData> nodeMap;
    private static boolean printFlag = true;// flag to stop print

    static {
        nodeMap = new TreeMap<>();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        ServerSocket branchSocket = null;
        Socket clientSocket = null;
        if (args.length == 2) {
            filePath = args[0];
            System.out.println("Client Started");
            FileProcessor fPro = new FileProcessor(filePath);
            String str = "";
            while ((str = fPro.readLine()) != null) {
                ServerData nodeServerData = new ServerData(str);
                nodeMap.put(nodeServerData.getName(), nodeServerData);
            }
            print(nodeMap);



            for (int i = 1; i < 256; i++) {
                sendPUTRequestToCoordinator("node"+args[1],i, "XYjZO" +i+args[1], MyCassandra.ConsistencyLevel.TWO);

                //Thread.sleep(5000);
                sendGETRequestToCoordinator("node"+args[1],i,  MyCassandra.ConsistencyLevel.ONE);

                sendGETRequestToCoordinator("node"+args[1], i+1, MyCassandra.ConsistencyLevel.TWO);
            }

            // for (int i = 0; i < 100; i++)
//            sendPUTRequestToCoordinator("node1", 9, "AC", MyCassandra.ConsistencyLevel.TWO);
//            sendGETRequestToCoordinator("node5", 0, MyCassandra.ConsistencyLevel.TWO);

        }
    }

    public static int get(int num) {

        return ((nodeMap.size()) + num) % (nodeMap.size());
    }

    private static void sendPUTRequestToCoordinator(String node, int key, String value, MyCassandra.ConsistencyLevel consistencyLevel) {
        try {
            System.out.println("IP: " + nodeMap.get(node).getIp() + " Port:" + nodeMap.get(node).getPort());
            Socket socket = null;

            socket = new Socket(nodeMap.get(node).getIp(), nodeMap.get(node).getPort());

            MyCassandra.ClientWriteRequest.Builder putKeyVal = MyCassandra.ClientWriteRequest.newBuilder();
            putKeyVal.setKey(key).setValue(value).setConsistencyLevel(consistencyLevel).build();
            MyCassandra.WrapperMessage.Builder msg = MyCassandra.WrapperMessage.newBuilder();
            msg.setClientWriteRequest(putKeyVal).build().writeDelimitedTo(socket.getOutputStream());


            MyCassandra.WrapperMessage message = MyCassandra.WrapperMessage.parseDelimitedFrom(socket.getInputStream());
            System.out.println("Message Received : " + message);
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void sendGETRequestToCoordinator(String node, int key, MyCassandra.ConsistencyLevel consistencyLevel) {
        try {
            System.out.println("IP: " + nodeMap.get(node).getIp() + " Port:" + nodeMap.get(node).getPort());
            Socket socket = null;

            socket = new Socket(nodeMap.get(node).getIp(), nodeMap.get(node).getPort());

            MyCassandra.ClientReadRequest.Builder getKeyVal = MyCassandra.ClientReadRequest.newBuilder();
            getKeyVal.setKey(key).setConsistencyLevel(consistencyLevel).build();
            MyCassandra.WrapperMessage.Builder msg = MyCassandra.WrapperMessage.newBuilder();
            msg.setClientReadRequest(getKeyVal).build().writeDelimitedTo(socket.getOutputStream());


            MyCassandra.WrapperMessage message = MyCassandra.WrapperMessage.parseDelimitedFrom(socket.getInputStream());
            System.out.println("Message Received : " + message);
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
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
