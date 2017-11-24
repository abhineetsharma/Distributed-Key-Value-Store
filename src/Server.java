
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class Server {

    private int portNumber;
    private String name;
    private String filePath;
    private String ip;
    private  Map<String, NodeServerData> nodeMap;
    private  Map<String,ValueMetaData> keyValueMap;
    private static boolean printFlag = true;//flag to stop print

    private void initServer() {
        //server information read by the server read by server
        FileProcessor fPro = new FileProcessor(filePath);
        nodeMap = new TreeMap<>();
        String str = "";
        while ((str = fPro.readLine()) != null) {
            NodeServerData nodeServerData = new NodeServerData(str);
            nodeMap.put(nodeServerData.getName(), nodeServerData);

            if (nodeServerData.getPort() == portNumber) {
                name = nodeServerData.getName();
                ip = nodeServerData.getIp();
            }
        }
        print(nodeMap);
        print(getReplicaServersList("node4"));

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

    private List<NodeServerData> getReplicaServersList(String keyNode) {
        String[] mapKeys = this.nodeMap.keySet().toArray(new String[nodeMap.size()]);
        int keyPosition = Arrays.asList(mapKeys).indexOf(keyNode);
        List<NodeServerData> nodeServerDataList = new ArrayList<>();

        for (int i = 0; i < 4 && i < nodeMap.size(); i++) {
            nodeServerDataList.add(nodeMap.get(mapKeys[(nodeMap.size() + keyPosition + i) % nodeMap.size()]));
        }

        return nodeServerDataList;
    }


    public static void main(String[] args) {

    	Server server = new Server();

        if (args.length > 1) {
            server.portNumber = Integer.parseInt(args[0]);
            server.filePath = args[1];

        }
        else {
        	System.out.println("Invalid number of arguments to controller");
			System.exit(0);
        }

        server.initServer();

        ServerSocket serverSocket = null;
            try {
				serverSocket = new ServerSocket(server.portNumber);
			} catch (IOException ex) {
	            System.out.println("Server socket cannot be created");
	            ex.printStackTrace();
	            System.exit(1);
	        }

            while(true){
    			Socket receiver = null;
    			try{
    				receiver = serverSocket.accept();
    				Node.WrapperMessage message = Node.WrapperMessage.parseDelimitedFrom(receiver.getInputStream());
					print(message);

    				if (message.hasClientReadRequest()) {
    					//call appropriate method from here
    					receiver.close();
    				}
    				else if (message.hasClientWriteRequest()) {
    					//call appropriate method from here
    					receiver.close();
    				}
    				else if (message.hasGetKeyFromCoordinator()) {
    					//call appropriate method from here
    					receiver.close();
    				}
    				else if (message.hasPutKeyFromCoordinator()) {
    					//call appropriate method from here
    					receiver.close();
    				}
    				else if (message.hasReadRepair()) {
    					//call appropriate method from here
    					receiver.close();
    				}
    			}catch (IOException e) {
    				System.out.println("Error reading data from socket. Exiting main thread");
    				e.printStackTrace();
    				System.exit(1);
    			}
    	  }
    }

    private static String getCurrentTimeString(){
        return Long.toString(System.currentTimeMillis());
    }


}