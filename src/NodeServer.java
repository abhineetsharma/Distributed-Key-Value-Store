
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;

public class NodeServer {

    static int portNumber;
    static String nodeName;
    private static Map<String, MapMessage.InitReplica.Replica> NodeMap;

    static {
        NodeMap = new TreehMap<>();
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
                            NodeServer.MapMessage msg = Map.MapMessage.parseDelimitedFrom(is);


                }
            }
        } catch (UnknownHostException e1) {
            e1.printStackTrace();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }


}