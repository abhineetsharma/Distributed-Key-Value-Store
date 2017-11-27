
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;


public class Server {

    private int portNumber;
    private String name;
    private String filePath;
    private String ip;
    private Map<String, NodeServerData> nodeMap;
    private Map<Integer, ValueMetaData> keyValueMap;
    private Map<String, AcknowledgementToClientListener> acknowledgementLogCoordinatorMap;
    private String logFileName;
    private static boolean printFlag = true;// flag to stop print

    private void initServer() {
        // server information read by the server read by server
        FileProcessor fPro = new FileProcessor(filePath);
        nodeMap = new TreeMap<>();
        keyValueMap = new ConcurrentSkipListMap<>();
        acknowledgementLogCoordinatorMap = new ConcurrentSkipListMap<>();
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

    //to get the replica server list
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

    //Client Read Request
    private void processingClientReadRequest(Node.ClientReadRequest clientReadRequest, Socket clientSocket) {
        int key = clientReadRequest.getKey();
        //String value = clientReadRequest.getValue();
        Node.ConsistencyLevel clientConsistencyLevel = clientReadRequest.getConsistencyLevel();

        NodeServerData primaryReplica = getNodeByKey(key);

        print(primaryReplica.toString());
        List<NodeServerData> replicaServerList = getReplicaServersList(primaryReplica.getName());
        print(replicaServerList);

        String timeStamp = getCurrentTimeString();

        acknowledgementLogCoordinatorMap.put(timeStamp, new AcknowledgementToClientListener(null, clientSocket, clientConsistencyLevel, timeStamp, key, null, replicaServerList));

        for (NodeServerData replica : replicaServerList) {

            Node.GetKeyFromCoordinator.Builder getKeyFromCoordinatorBuilder = Node.GetKeyFromCoordinator.newBuilder();

            getKeyFromCoordinatorBuilder.setKey(key);
            getKeyFromCoordinatorBuilder.setTimeStamp(timeStamp);
            getKeyFromCoordinatorBuilder.setCoordinatorName(name);

            Node.WrapperMessage message = Node.WrapperMessage.newBuilder()
                    .setGetKeyFromCoordinator(getKeyFromCoordinatorBuilder).build();

            try {
                sendMessageToNodeSocket(replica, message);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //Client Write Request
    private void processingClientWriteRequest(Node.ClientWriteRequest clientWriteRequest, Socket clientSocket) {
        int key = clientWriteRequest.getKey();
        String value = clientWriteRequest.getValue();
        Node.ConsistencyLevel clientConsistencyLevel = clientWriteRequest.getConsistencyLevel();

        NodeServerData primaryReplica = getNodeByKey(key);

        print(primaryReplica.toString());
        List<NodeServerData> replicaServerList = getReplicaServersList(primaryReplica.getName());
        print(replicaServerList);

        String timeStamp = getCurrentTimeString();


        acknowledgementLogCoordinatorMap.put(timeStamp, new AcknowledgementToClientListener(null, clientSocket, clientConsistencyLevel, timeStamp, key, value, replicaServerList));

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

    //Get Key From Coordinator
    private void processingGetKeyFromCoordinator(Node.GetKeyFromCoordinator getKeyFromCoordinator) {
        int key = getKeyFromCoordinator.getKey();
        String timeStamp = getKeyFromCoordinator.getTimeStamp();
        String coordinatorName = getKeyFromCoordinator.getCoordinatorName();


        Node.AcknowledgementToCoordinator.Builder acknowledgementBuilder = Node.AcknowledgementToCoordinator.newBuilder();
        acknowledgementBuilder.setKey(key);
        acknowledgementBuilder.setReplicaName(name);
        acknowledgementBuilder.setRequestType(Node.RequestType.READ);
        acknowledgementBuilder.setCoordinatorTimeStamp(timeStamp);
        if (keyValueMap.containsKey(key)) {
            String value = keyValueMap.get(key).getValue();
            String replicaTimeStamp = keyValueMap.get(key).getTimeStamp();

            print(keyValueMap.get(key));


            acknowledgementBuilder.setReplicaTimeStamp(replicaTimeStamp);
            acknowledgementBuilder.setValue(value);


            System.out.println("Read Ack message sent to the Coordinator : " + coordinatorName + " .....");
        } else {
            acknowledgementBuilder.setErrorMessage("Key " + key + " not found ");
        }

        Node.WrapperMessage message = Node.WrapperMessage.newBuilder().setAcknowledgementToCoordinator(acknowledgementBuilder).build();

        NodeServerData coordinator = nodeMap.get(coordinatorName);

        // send acknowledgement to coordinator
        try {
            sendMessageToNodeSocket(coordinator, message);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //Put Key From Coordinator Request
    private void processingPutKeyFromCoordinatorRequest(Node.PutKeyFromCoordinator putKeyFromCoordinator)
            throws IOException {
        int key = putKeyFromCoordinator.getKey();
        String value = putKeyFromCoordinator.getValue();
        String timeStamp = putKeyFromCoordinator.getTimeStamp();
        String coordinatorName = putKeyFromCoordinator.getCoordinatorName();

        // BEGINE : Nikhil's pre commite code
        // Writting to log file.
        StringBuilder builder = new StringBuilder();
        this.logFileName = builder.append(this.name).append("LogFile.txt").toString();

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
                acknowledgementBuilder.setReplicaTimeStamp(timeStamp);
                acknowledgementBuilder.setCoordinatorTimeStamp(timeStamp);
                acknowledgementBuilder.setValue(value);
                acknowledgementBuilder.setReplicaName(name);
                acknowledgementBuilder.setRequestType(Node.RequestType.WRITE);

                Node.WrapperMessage message = Node.WrapperMessage.newBuilder()
                        .setAcknowledgementToCoordinator(acknowledgementBuilder).build();

                NodeServerData coordinator = nodeMap.get(coordinatorName);

                // send acknowledgement to coordinator
                sendMessageToNodeSocket(coordinator, message);
                System.out.println("Write Ack message send to Coordinator : " + coordinatorName + " .....");
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

    //Acknowledgement To Coordinator
    private synchronized void processingAcknowledgementToCoordinator(Node.AcknowledgementToCoordinator acknowledgementToCoordinator) {
        int key = acknowledgementToCoordinator.getKey();
        String value = acknowledgementToCoordinator.getValue();

        Node.RequestType requestType = acknowledgementToCoordinator.getRequestType();
        String replicaName = acknowledgementToCoordinator.getReplicaName();
        String replicasCoordinatorTimeStamp = acknowledgementToCoordinator.getCoordinatorTimeStamp();
        String replicaTimeStamp = acknowledgementToCoordinator.getReplicaTimeStamp();
        AcknowledgementToClientListener acknowledgement = acknowledgementLogCoordinatorMap.get(replicasCoordinatorTimeStamp);

        if (null != acknowledgement) {
            Node.ConsistencyLevel consistencyLevel = acknowledgement.getRequestConsistencyLevel();
            Socket clientSocket = acknowledgement.getClientSocket();
            boolean isSentToClient = acknowledgement.isSentToClient();
            AcknowledgementData acknowledgementData = acknowledgement.getAcknowledgementDataByServerName(replicaName);
            List<String> replicaAcknowledgementList = acknowledgement.getAcknowledgedListForTimeStamp();
            String errorMessage = acknowledgementToCoordinator.getErrorMessage();
            if (errorMessage == null) errorMessage = "";
            if (null != acknowledgementData) {
                int replicaKey = acknowledgementData.getKey();
                String replicaValue = acknowledgementData.getValue();

                if (requestType.equals(Node.RequestType.WRITE)) {
                    if (key == replicaKey && replicaValue.equalsIgnoreCase(value)) {
                        acknowledgementData.setAcknowledge(true);
                    } else {
                        //inconsistency
                    }
                }
                //Read request
                else if (requestType.equals(Node.RequestType.READ)) {
                    if (value == null && errorMessage.trim().length() > 0) {
                        //value is null because key does not exist

                    } else {
                        acknowledgement.setValueFromReplicaAcknowledgement(replicaName, value);
                        acknowledgement.setTimeStampFromReplica(replicaName, replicaTimeStamp);
                        //set the time stamp of last write operation into Acknowledgement log
                        acknowledgementData.setAcknowledge(true);
                    }
                }

                int acknowledgeCount = replicaAcknowledgementList.size();

                //check
                if (requestType.equals(Node.RequestType.WRITE)) {
                    if (acknowledgeCount >= consistencyLevel.getNumber() && !isSentToClient) {
                        acknowledgement.setSentToClient(true);
                        sentAcknowledgementToClient(key, value, errorMessage, clientSocket);
                    }

                } else if (requestType.equals(Node.RequestType.READ)) {
                    //un-comment to change time stamp of particular replica
                    //                    if (replicaName.equalsIgnoreCase("node2")) {
                    //                        acknowledgement.setValueFromReplicaAcknowledgement(replicaName, value + "abhineet");
                    //                        acknowledgement.setTimeStampFromReplica(replicaName, Long.toString(Long.parseLong(replicaTimeStamp) + 1));
                    //                    }
                    if (acknowledgeCount >= consistencyLevel.getNumber() && !isSentToClient) {
                        AcknowledgementData acknowledgeData = acknowledgement.getAcknowledgementDataByServerName(replicaAcknowledgementList.get(0));
                        System.out.println(">>>>>>>>>Replica with latest data : "+acknowledgeData.getReplicaName()+" Time stamp : "+acknowledgeData.getTimeStamp()+" Value : " +acknowledgeData.getValue() );
                        acknowledgement.setSentToClient(true);
                        
                		new Thread(new Runnable() {
                			@Override
                			public void run() {
                				readRepair();
                			}
                		}).start();

                        sentAcknowledgementToClient(key, acknowledgeData.getValue(), errorMessage, clientSocket);
                    } else {
                        System.out.println("err");
                    }

                }

            }
        }
    }
    
    public void readRepair() {

		Timer timer = new Timer();
		class transferClass extends TimerTask {
			@Override
			public void run() {
				{
					for (Entry<String, AcknowledgementToClientListener> e : acknowledgementLogCoordinatorMap.entrySet()) {
						AcknowledgementToClientListener allReplicasEntry = e.getValue();
						if(allReplicasEntry.isSentToClient()) {
							List<String> replicaAcknowledgementList = allReplicasEntry.getAcknowledgedListForTimeStamp();
							AcknowledgementData LatestTimeStampedReplicaData = allReplicasEntry.getAcknowledgementDataByServerName(replicaAcknowledgementList.get(0));
							for(int i=1;i<4;i++) {
								AcknowledgementData conflictingReplica = allReplicasEntry.getAcknowledgementDataByServerName(replicaAcknowledgementList.get(i));
								if(!LatestTimeStampedReplicaData.getTimeStamp().equals(conflictingReplica.getTimeStamp())){
									Node.PutKeyFromCoordinator.Builder putKeyFromCoordinatorToConflictingReplicaBuilder = Node.PutKeyFromCoordinator.newBuilder();
									putKeyFromCoordinatorToConflictingReplicaBuilder.setKey(LatestTimeStampedReplicaData.getKey());
									putKeyFromCoordinatorToConflictingReplicaBuilder.setTimeStamp(LatestTimeStampedReplicaData.getTimeStamp());
									putKeyFromCoordinatorToConflictingReplicaBuilder.setValue(LatestTimeStampedReplicaData.getValue());
									putKeyFromCoordinatorToConflictingReplicaBuilder.setCoordinatorName(LatestTimeStampedReplicaData.getReplicaName());
									Node.WrapperMessage message = Node.WrapperMessage.newBuilder().setPutKeyFromCoordinator(putKeyFromCoordinatorToConflictingReplicaBuilder).build();
									try {
										sendMessageToNodeSocket(nodeMap.get(conflictingReplica.getReplicaName()), message);
									} catch (IOException ex) {
										ex.printStackTrace();
									}		
								}
							}


						}

					}
					//					timer.cancel();
					timer.schedule(new transferClass(), (5000));

					try {


					} catch (Exception e) {
						e.printStackTrace();
						System.err.println("Can not transfer money");
					}
				}
			}
		}
		;
		new transferClass().run();
	
    }

    private void sentAcknowledgementToClient(int key, String value, String errorMessage, Socket clientSocket) {
        Node.AcknowledgementToClient acknowledgementToClient = Node.AcknowledgementToClient.newBuilder().setKey(key).setValue(value).setErrorMessage(errorMessage).build();
        Node.WrapperMessage message = Node.WrapperMessage.newBuilder().setAcknowledgementToClient(acknowledgementToClient).build();
        print("Send to client: " + message + " " + clientSocket.isClosed());
        try {
            if (clientSocket != null && !clientSocket.isClosed()) {
                print(acknowledgementToClient);
                message.writeDelimitedTo(clientSocket.getOutputStream());
                clientSocket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
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
                        server.processingClientReadRequest(message.getClientReadRequest(), receiver);
                        print("----ClientReadRequest End----");
                    } else if (message.hasClientWriteRequest()) {
                        print("----ClientWriteRequest Start----");
                        // call appropriate method from here
                        server.processingClientWriteRequest(message.getClientWriteRequest(), receiver);
                        print("----ClientWriteRequest End----");
                    } else if (message.hasGetKeyFromCoordinator()) {
                        print("----GetKeyFromCoordinator Start----");
                        // call appropriate method from here
                        server.processingGetKeyFromCoordinator(message.getGetKeyFromCoordinator());
                        receiver.close();
                        print("----GetKeyFromCoordinator End----");
                    } else if (message.hasPutKeyFromCoordinator()) {
                        print("----PutKeyFromCoordinator Start----");
                        // call appropriate method from here
                        server.processingPutKeyFromCoordinatorRequest(message.getPutKeyFromCoordinator());
                        receiver.close();
                        print("----PutKeyFromCoordinator End----");
                    } else if (message.hasAcknowledgementToCoordinator()) {
                        print("----Acknowledgement To Coordinator Start----");
                        server.processingAcknowledgementToCoordinator(message.getAcknowledgementToCoordinator());
                        receiver.close();
                        print("----Acknowledgement To Coordinator End----");
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
//                    try {
//                        //receiver.close();
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
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