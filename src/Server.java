
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;


public class Server {

    // this server details
    private int portNumber;
    private String name;
    private String ip;
    private int replicaFactor;

    // file paths
    private String nodeFilePath;
    private String logFilePath;
    private String hintedhandOffFilePath;

    // data structures
    private Map<String, ServerData> allServersData;
    private Map<Integer, ValueMetaData> keyValueDataStore;
    private Map<String, AcknowledgementToClientListener> acknowledgementLogCoordinatorMap;
    private Map<String, List<ConflictingReplica>> failedWriteRequests;

    // flag to stop print
    private static boolean printFlag = true;

    private void initServer() {
        // read Node.txt and write into allServerData
        FileProcessor fPro = new FileProcessor(nodeFilePath);
        allServersData = new TreeMap<>();
        keyValueDataStore = new ConcurrentSkipListMap<>();
        acknowledgementLogCoordinatorMap = new ConcurrentSkipListMap<>();
        failedWriteRequests = new HashMap<>();
        replicaFactor = 4;
        String str = "";
        try {
            if (fPro.countLines() >= replicaFactor) {
                while ((str = fPro.readLine()) != null) {
                    ServerData nodeServerData = new ServerData(str);
                    allServersData.put(nodeServerData.getName(), nodeServerData);

                    if (nodeServerData.getPort() == portNumber) {
                        name = nodeServerData.getName();
                        ip = nodeServerData.getIp();
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // read logFile for recovery from failure
        File file = new File(logFilePath);
        if (file.exists()) {
            Node.LogBook.Builder logBook = Node.LogBook.newBuilder();

            // Read the existing Log book.
            try {
                logBook.mergeFrom(new FileInputStream(logFilePath));
            } catch (FileNotFoundException e) {
                System.out.println(": File not found.  Creating a new file.");
            } catch (IOException ex) {
                System.out.println("Error reading log file");
                ex.printStackTrace();
            }

            // copy log messages with log-end flag = 1 to key-value Map
            for (Node.LogMessage log : logBook.getLogList()) {
                if (log.getLogEndFlag()) {
                    if (keyValueDataStore.containsKey(log.getKey())) {
                        // If key is present in our keyValue store then compare time-stamps
                        keyValueDataStore.get(log.getKey()).updateKeyWithValue(log.getTimeStamp(), log.getValue());
                    } else {
                        keyValueDataStore.put(log.getKey(), new ValueMetaData(log.getTimeStamp(), log.getValue()));
                    }
                }
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


    //to get the replica server list
    private List<ServerData> getReplicaServersList(String keyNode) {
        String[] mapKeys = allServersData.keySet().toArray(new String[allServersData.size()]);
        int keyPosition = Arrays.asList(mapKeys).indexOf(keyNode);
        List<ServerData> nodeServerDataList = new ArrayList<>();

        for (int i = 0; i < replicaFactor && i < allServersData.size(); i++) {
            nodeServerDataList
                    .add(allServersData
                            .get(mapKeys[(allServersData.size() + keyPosition + i) % allServersData.size()]));
        }
        return nodeServerDataList;
    }

    private ServerData getPrimaryReplicaForKey(int key) {
        int num = (key + allServersData.size()) % allServersData.size();
        String[] nodeKeys = allServersData.keySet().toArray(new String[allServersData.size()]);
        return allServersData.get(nodeKeys[num]);
    }

    // coordinator processing client's read request
    private void processingClientReadRequest(Node.ClientReadRequest clientReadRequest, Socket clientSocket) {
        int key = clientReadRequest.getKey();
        //String value = clientReadRequest.getValue();
        Node.ConsistencyLevel clientConsistencyLevel = clientReadRequest.getConsistencyLevel();

        ServerData primaryReplica = getPrimaryReplicaForKey(key);

        print(primaryReplica.toString());
        List<ServerData> replicaServerList = getReplicaServersList(primaryReplica.getName());
        print(replicaServerList);

        String timeStamp = getCurrentTimeString();

        acknowledgementLogCoordinatorMap.put(timeStamp,
                new AcknowledgementToClientListener(null, clientSocket, clientConsistencyLevel, timeStamp, key, null, replicaServerList));

        int messageSentCounterToReplica = 0;
        for (ServerData replica : replicaServerList) {

            Node.GetKeyFromCoordinator.Builder getKeyFromCoordinatorBuilder = Node.GetKeyFromCoordinator.newBuilder();

            getKeyFromCoordinatorBuilder.setKey(key);
            getKeyFromCoordinatorBuilder.setTimeStamp(timeStamp);
            getKeyFromCoordinatorBuilder.setCoordinatorName(name);

            Node.WrapperMessage message = Node.WrapperMessage.newBuilder()
                    .setGetKeyFromCoordinator(getKeyFromCoordinatorBuilder).build();

            try {
                sendMessageViaSocket(replica, message);
                messageSentCounterToReplica++;
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        if (messageSentCounterToReplica < clientConsistencyLevel.getNumber()) {
            //return client an error message
            String errorMessage = "Cannot find the key " + key;
            sentAcknowledgementToClient(key, null, errorMessage, clientSocket);
            acknowledgementLogCoordinatorMap.remove(timeStamp);
        }
    }

    // coordinator processing client's write request
    private void processingClientWriteRequest(Node.ClientWriteRequest clientWriteRequest, Socket clientSocket) {
        int key = clientWriteRequest.getKey();
        String value = clientWriteRequest.getValue();
        Node.ConsistencyLevel clientConsistencyLevel = clientWriteRequest.getConsistencyLevel();

        ServerData primaryReplica = getPrimaryReplicaForKey(key);

        print(primaryReplica.toString());
        List<ServerData> replicaServerList = getReplicaServersList(primaryReplica.getName());
        print(replicaServerList);

        String timeStamp = getCurrentTimeString();
        acknowledgementLogCoordinatorMap.put(timeStamp, new AcknowledgementToClientListener(null, clientSocket, clientConsistencyLevel, timeStamp, key, value, replicaServerList));

        for (ServerData replica : replicaServerList) {

            Node.PutKeyFromCoordinator.Builder putKeyFromCoordinatorBuilder = Node.PutKeyFromCoordinator.newBuilder();

            putKeyFromCoordinatorBuilder.setKey(key);
            putKeyFromCoordinatorBuilder.setTimeStamp(timeStamp);
            putKeyFromCoordinatorBuilder.setValue(value);
            putKeyFromCoordinatorBuilder.setCoordinatorName(name);
            putKeyFromCoordinatorBuilder.setIsReadRepair(false);

            Node.WrapperMessage message = Node.WrapperMessage.newBuilder()
                    .setPutKeyFromCoordinator(putKeyFromCoordinatorBuilder).build();

            try {
                sendMessageViaSocket(replica, message);
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("replica server " + replica.getName() + " down");
                ConflictingReplica conflictingReplica = new ConflictingReplica(replica, message);
                addFailedWriteRequestForServer(replica, conflictingReplica);
            }
        }
    }

    private void addFailedWriteRequestForServer(ServerData replica, ConflictingReplica conflictingReplica) {
        String serverName = replica.getName();
        if (failedWriteRequests.containsKey(serverName)) {
            List<ConflictingReplica> messages = failedWriteRequests.get(serverName);
            messages.add(conflictingReplica);
        } else {
            List<ConflictingReplica> messages = new ArrayList<>();
            messages.add(conflictingReplica);
            failedWriteRequests.put(serverName, messages);
        }
    }

    // normal server processing get key request from co-ordinator
    private void processingGetKeyFromCoordinator(Node.GetKeyFromCoordinator getKeyFromCoordinator) {
        int key = getKeyFromCoordinator.getKey();
        String timeStamp = getKeyFromCoordinator.getTimeStamp();
        String coordinatorName = getKeyFromCoordinator.getCoordinatorName();


        Node.AcknowledgementToCoordinator.Builder acknowledgementBuilder = Node.AcknowledgementToCoordinator.newBuilder();

        acknowledgementBuilder.setKey(key);
        acknowledgementBuilder.setReplicaName(name);
        acknowledgementBuilder.setRequestType(Node.RequestType.READ);
        acknowledgementBuilder.setCoordinatorTimeStamp(timeStamp);

        if (keyValueDataStore.containsKey(key)) {
            ValueMetaData data = keyValueDataStore.get(key);
            String value = data.getValue();
            String replicaTimeStamp = data.getTimeStamp();

            print(data);

            acknowledgementBuilder.setReplicaTimeStamp(replicaTimeStamp);
            acknowledgementBuilder.setValue(value);

        } else {
            acknowledgementBuilder.setErrorMessage("Key " + key + " not found ");
        }

        Node.WrapperMessage message = Node.WrapperMessage.newBuilder().setAcknowledgementToCoordinator(acknowledgementBuilder).build();

        ServerData coordinator = allServersData.get(coordinatorName);

        // send acknowledgement to coordinator
        try {
            sendMessageViaSocket(coordinator, message);
            System.out.println("Read Ack message sent to the Coordinator : " + coordinatorName + " .....");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // normal server processing putkey request from co-ordinator
    private void processingPutKeyFromCoordinatorRequest(Node.PutKeyFromCoordinator putKeyFromCoordinator) {
        int putKey = putKeyFromCoordinator.getKey();
        String putValue = putKeyFromCoordinator.getValue();
        String putTimeStamp = putKeyFromCoordinator.getTimeStamp();
        String coordinatorName = putKeyFromCoordinator.getCoordinatorName();

        // BEGIN : Nikhil's pre commite code

        Node.LogMessage logMessage = Node.LogMessage.newBuilder().setKey(putKey)
                .setValue(putValue).setTimeStamp(putTimeStamp)
                .setLogEndFlag(false).build();

        Node.LogBook.Builder logBook = Node.LogBook.newBuilder();

        // Read the existing Log book.
        try {
            logBook.mergeFrom(new FileInputStream(logFilePath));
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
            output = new FileOutputStream(logFilePath);
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
            logBook.mergeFrom(new FileInputStream(logFilePath));
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
                int logKey = log.getKey();
                String logTimeStamp = log.getTimeStamp();
                String logValue = log.getValue();

                if (keyValueDataStore.containsKey(logKey)) {
                    //If key is present in our keyValue store then compare time-stamps
                    keyValueDataStore.get(logKey).updateKeyWithValue(logTimeStamp, logValue);
                } else {
                    keyValueDataStore.put(logKey, new ValueMetaData(logTimeStamp, logValue));
                }
                // do post commit processing via log file and code
                // update log message and add to newLogBook
                Node.LogMessage newLogMessage = Node.LogMessage.newBuilder().setKey(logKey)
                        .setValue(logValue).setTimeStamp(logTimeStamp).setLogEndFlag(true).build();
                newLogBook.addLog(newLogMessage);

                print(keyValueDataStore.get(logKey));
                // Create acknowledgement and message to coordinator

                if (!putKeyFromCoordinator.getIsReadRepair()) {
                    Node.AcknowledgementToCoordinator acknowledgement = Node.AcknowledgementToCoordinator
                            .newBuilder()
                            .setKey(logKey)
                            .setReplicaTimeStamp(logTimeStamp)
                            .setCoordinatorTimeStamp(logTimeStamp)
                            .setValue(logValue)
                            .setReplicaName(name)
                            .setRequestType(Node.RequestType.WRITE).build();

                    Node.WrapperMessage message = Node.WrapperMessage
                            .newBuilder()
                            .setAcknowledgementToCoordinator(acknowledgement)
                            .build();

                    ServerData coordinator = allServersData.get(coordinatorName);

                    // send acknowledgement to coordinator
                    try {
                        sendMessageViaSocket(coordinator, message);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    System.out.println("Write Ack message send to Coordinator : " + coordinatorName + " .....");
                }
            }

            // Write updated logbook back to disk.
            try {
                output = new FileOutputStream(logFilePath);
                newLogBook.build().writeTo(output);
                output.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    // normal server sending acknowledgement To Coordinator
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

            String errorMessage = acknowledgementToCoordinator.getErrorMessage();
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
                    if (value == null && errorMessage != null && errorMessage.trim().length() > 0) {
                        //value is null because key does not exist

                    } else {
                        acknowledgement.setTimeStampAndValueFromReplica(replicaName, replicaTimeStamp, value);
                        //set the time stamp of last write operation into Acknowledgement log
                        acknowledgementData.setAcknowledge(true);
                    }
                }
                List<String> replicaAcknowledgementList = acknowledgement.getAcknowledgedListForTimeStamp();
                int acknowledgeCount = replicaAcknowledgementList.size();

                //check
                if (requestType.equals(Node.RequestType.WRITE)) {
                    if (acknowledgeCount >= consistencyLevel.getNumber() && !isSentToClient) {
                        acknowledgement.setSentToClient(true);
                        sentAcknowledgementToClient(key, value, errorMessage, clientSocket);
                    }

                } else if (requestType.equals(Node.RequestType.READ)) {
                    //un-comment to change time stamp of particular replica
                    //if (replicaName.equalsIgnoreCase("node2") && debugFlag && replicaTimeStamp != null && replicaTimeStamp.trim().length() > 0) {
                    //    debugFlag = false;
                    //    acknowledgement.setValueFromReplicaAcknowledgement(replicaName, value + "abhineet");
                    //    acknowledgement.setTimeStampFromReplica(replicaName, Long.toString(Long.parseLong(replicaTimeStamp) - 10000));
                    //}
                    if (acknowledgeCount >= consistencyLevel.getNumber() && !isSentToClient) {
                        String mostRecentReplicaName = replicaAcknowledgementList.get(0);
                        AcknowledgementData acknowledgeData = acknowledgement.getAcknowledgementDataByServerName(mostRecentReplicaName);
                        print("Replica with latest data : " + acknowledgeData.getReplicaName() + " Time stamp : " + acknowledgeData.getTimeStamp() + " Value : " + acknowledgeData.getValue());
                        acknowledgement.setSentToClient(true);

                        sentAcknowledgementToClient(key, acknowledgeData.getValue(), errorMessage, clientSocket);
                    }

                    if (isSentToClient && acknowledgement.isInconsistent()) {
                        print("----Read Repair Start----");
                        processReadRepair(acknowledgement);
                        print("----Read Repair End------");
                    }
                }
            }
        }
    }

    //uncomment for the above if debug test
    //boolean debugFlag = true;

    //Read repair thread
    private synchronized void processReadRepair(AcknowledgementToClientListener acknowledgement) {
        Thread thread = new Thread(() -> {

            List<String> replicaAcknowledgementList = acknowledgement.getAcknowledgedListForTimeStamp();

            String mostRecentValueReplicaName = replicaAcknowledgementList.get(0);
            AcknowledgementData mostRecentValueFromReplica = acknowledgement.getAcknowledgementDataByServerName(mostRecentValueReplicaName);

            replicaAcknowledgementList.remove(mostRecentValueReplicaName);

            for (String serverName : replicaAcknowledgementList) {
                AcknowledgementData acknowledgementData = acknowledgement.getAcknowledgementDataByServerName(serverName);
                if (!mostRecentValueFromReplica.getValue().equalsIgnoreCase(acknowledgementData.getValue())) {
                    Node.PutKeyFromCoordinator.Builder putKeyFromCoordinatorToConflictingReplicaBuilder = Node.PutKeyFromCoordinator.newBuilder();

                    putKeyFromCoordinatorToConflictingReplicaBuilder.setKey(mostRecentValueFromReplica.getKey())
                            .setTimeStamp(mostRecentValueFromReplica.getTimeStamp())
                            .setValue(mostRecentValueFromReplica.getValue())
                            .setCoordinatorName(mostRecentValueFromReplica.getReplicaName())
                            .setIsReadRepair(true);

                    Node.WrapperMessage message = Node.WrapperMessage
                            .newBuilder()
                            .setPutKeyFromCoordinator(putKeyFromCoordinatorToConflictingReplicaBuilder)
                            .build();

                    ServerData replicaServer = allServersData.get(serverName);

                    try {
                        print("Read repair message sent to " + replicaServer);
                        sendMessageViaSocket(replicaServer, message);
                    } catch (IOException ex) {
                        ex.printStackTrace();
                        ConflictingReplica conflictingServer = new ConflictingReplica(replicaServer, message);
                        addFailedWriteRequestForServer(replicaServer, conflictingServer);
                    }
                }
            }

        });
        thread.start();
    }

    // coordinator sending acknowledgement to client
    private void sentAcknowledgementToClient(int key, String value, String errorMessage, Socket clientSocket) {
        if (value == null)
            value = "";
        if (errorMessage == null) {
            errorMessage = "";
        }
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

    private void sendMessageViaSocket(ServerData serverData, Node.WrapperMessage wrapperMessage) throws IOException {
        print("----Socket message Start----");
        Socket socket = new Socket(serverData.getIp(), serverData.getPort());
        wrapperMessage.writeDelimitedTo(socket.getOutputStream());
        socket.close();
        print("\n\nMessage sent via socket to " + serverData + "\nmessage \n" + wrapperMessage);
        print("----Socket message End-----");
    }

    private String getCurrentTimeString() {
        return Long.toString(System.currentTimeMillis());
    }

    private void sendPendingRequestsToCoordinator(String coordinatorName) {
    	if (this.failedWriteRequests.containsKey(coordinatorName)) {
			List<ConflictingReplica> failedWritesQueue = failedWriteRequests.get(coordinatorName);
			boolean sendSuccess;
			for (ConflictingReplica cr : failedWritesQueue) {
				try {
					sendSuccess = false;
					sendMessageViaSocket(cr.getNodeServerData(), cr.getMessage());
					sendSuccess = true;
					if (sendSuccess) {
						//delete the key-value from data structure	
						
						//read the old book
						Node.HintedHandOffBook hintedHandOffBook = Node.HintedHandOffBook.parseFrom(new FileInputStream(this.hintedhandOffFilePath));
						
						//modify the in-memory hintedHandOff data structure
						Node.HintedHandOffBook.Builder newHHFBook = Node.HintedHandOffBook.newBuilder();
						
						
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
    
    public static void main(String[] args) {

        Server server = new Server();

        File dir = new File("dir");
        if (!dir.exists()) {
            dir.mkdir();
        }

        // set server's log file path
        StringBuilder builder = new StringBuilder();
        try {
            server.logFilePath = dir.getCanonicalPath() + "/"
                    + builder.append(server.name).append("LogFile.txt").toString();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // set HintedHandoff log file path
        StringBuilder strbuilder = new StringBuilder();
        try {
			server.hintedhandOffFilePath = dir.getCanonicalPath() + "/" + strbuilder.append(server.name).append("hhf.txt").toString();
		} catch (IOException e1) {
			e1.printStackTrace();
		}

        if (args.length > 1) {
            server.portNumber = Integer.parseInt(args[0]);
            server.nodeFilePath = args[1];

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
                    //Client Read Request
                    if (message.hasClientReadRequest()) {
                        print("----ClientReadRequest Start----");
                        server.processingClientReadRequest(message.getClientReadRequest(), receiver);
                        print("----ClientReadRequest End----");
                    }
                    //Client Write Request
                    else if (message.hasClientWriteRequest()) {
                        print("----ClientWriteRequest Start----");
                        server.processingClientWriteRequest(message.getClientWriteRequest(), receiver);
                        print("----ClientWriteRequest End----");
                    }
                    //Get Key From Coordinator
                    else if (message.hasGetKeyFromCoordinator()) {
                        print("----GetKeyFromCoordinator Start----");
                        server.sendPendingRequestsToCoordinator(
								message.getGetKeyFromCoordinator().getCoordinatorName());
                        server.processingGetKeyFromCoordinator(message.getGetKeyFromCoordinator());
                        receiver.close();
                        print("----GetKeyFromCoordinator End----");
                    }
                    //Put Key From Coordinator
                    else if (message.hasPutKeyFromCoordinator()) {
                        print("----PutKeyFromCoordinator Start----");
                        server.sendPendingRequestsToCoordinator(
								message.getPutKeyFromCoordinator().getCoordinatorName());
                        server.processingPutKeyFromCoordinatorRequest(message.getPutKeyFromCoordinator());
                        receiver.close();
                        print("----PutKeyFromCoordinator End----");
                    }
                    //Acknowledgement To Coordinator
                    else if (message.hasAcknowledgementToCoordinator()) {
                        print("----Acknowledgement To Coordinator Start----");
                        server.processingAcknowledgementToCoordinator(message.getAcknowledgementToCoordinator());
                        receiver.close();
                        print("----Acknowledgement To Coordinator End----");
                    }
                }
            } catch (IOException e) {
                System.out.println("Error reading data from socket. Exiting main thread");
                e.printStackTrace();
                System.exit(1);
            } finally {
                print("\n\n---------------------------------------------");
                print("=====Message received End count = " + msgCount);
                print("---------------------------------------------\n\n");
            }
        }
    }
}