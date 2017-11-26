import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class AcknowledgementToClientListener {
    private String clientName;
    private Socket clientSocket;
    private Node.ConsistencyLevel requestConsistencyLevel;
    private boolean isSentToClient;
    private Map<String, AcknowledgementData> replicaAcknowledgementMap;


    public AcknowledgementToClientListener(String clientNameI, Socket clientSocketI, Node.ConsistencyLevel consistencyLevelI, String timeStampI, int keyI, String valueI, List<NodeServerData> nodeServerDataList) {
        clientName = clientNameI;
        clientSocket = clientSocketI;
        requestConsistencyLevel = consistencyLevelI;
        replicaAcknowledgementMap = new ConcurrentSkipListMap<>();
        for (NodeServerData node : nodeServerDataList)
            replicaAcknowledgementMap.put(node.getName(), new AcknowledgementData(keyI, valueI, timeStampI,node.getName()));
    }

    public String getClientName() {
        return clientName;
    }

    public Socket getClientSocket() {
        return clientSocket;
    }

    public Map<String, AcknowledgementData> getReplicaAcknowledgementMap() {
        return replicaAcknowledgementMap;
    }

    public AcknowledgementData getAcknowledgementDataByServerName(String nodeName) {
        return replicaAcknowledgementMap.get(nodeName);
    }

    public synchronized List<String> getAcknowledgedListForTimeStamp() {
        List<AcknowledgementData> AcknowledgementDataList = new ArrayList<>();
        for (String name : replicaAcknowledgementMap.keySet()) {
            AcknowledgementData data = replicaAcknowledgementMap.get(name);
            if (data.isAcknowledge())
                AcknowledgementDataList.add(data);
        }
        Collections.sort(AcknowledgementDataList);

        List<String> list = new ArrayList<>();
        for (AcknowledgementData acknowledgementData : AcknowledgementDataList) {
            list.add(acknowledgementData.getReplicaName());
        }
        //Collections.reverse(list);
        return list;
    }


    public Node.ConsistencyLevel getRequestConsistencyLevel() {
        return requestConsistencyLevel;
    }

    public boolean isSentToClient() {
        return isSentToClient;
    }

    public void setSentToClient(boolean sentToClient) {
        isSentToClient = sentToClient;
    }

    public synchronized void setValueFromReplicaAcknowledgement(String replicaName, String value) {
        AcknowledgementData acknowledgementData = getAcknowledgementDataByServerName(replicaName);
        acknowledgementData.setValue(value);
    }

    public synchronized void setTimeStampFromReplica(String replicaName, String timeStamp) {
        AcknowledgementData acknowledgementData = getAcknowledgementDataByServerName(replicaName);
        acknowledgementData.setTimeStamp(timeStamp);
    }

    public boolean isInconsistent() {
        Set<String> set = new HashSet<>();
        for (String replicaName : getAcknowledgedListForTimeStamp()) {
            AcknowledgementData data = replicaAcknowledgementMap.get(replicaName);
            set.add(data.getValue());
        }
        return set.size() == 1;
    }
}


