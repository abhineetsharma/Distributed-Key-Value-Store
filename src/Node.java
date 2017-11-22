
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Node {

    static int portNumber;
    static String nodeName;

    public static void main(String[] args) {

        ServerSocket branchSocket = null;
        Socket clientSocket = null;

        try {
            if (args.length > 1) {
                nodeName = args[0];
                portNumber = Integer.parseInt(args[1]);

                branchSocket = new ServerSocket(portNumber);
                System.out.println("Node Server Started");


                while (true) {
                    clientSocket = branchSocket.accept();

                    InputStream is = clientSocket.getInputStream();

                }
            }
        } catch (UnknownHostException e1) {
            e1.printStackTrace();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }


}