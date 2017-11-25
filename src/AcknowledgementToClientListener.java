import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class AcknowledgementToClientListener {
    private String clientName;
    private Socket clientSocket;
    private Node.ConsistencyLevel requestConsistencyLevel;
    private boolean isSentToClient;
    private Map<String, AcknowledgementData> replicaAcknowledgementMap;


    public AcknowledgementToClientListener(String clientNameI, Socket clientSocketI, Node.ConsistencyLevel consistencyLevelI, int keyI, String valueI, List<NodeServerData> nodeServerDataList) {
        clientName = clientNameI;
        clientSocket = clientSocketI;
        requestConsistencyLevel = consistencyLevelI;
        replicaAcknowledgementMap = new ConcurrentSkipListMap<>();
        for (NodeServerData node : nodeServerDataList)
            replicaAcknowledgementMap.put(node.getName(), new AcknowledgementData(keyI, valueI));
    }

    public String getClientName() {
        return clientName;
    }

    public Socket getClientScoket() {
        return clientSocket;
    }

    public Map<String, AcknowledgementData> getReplicaAcknowledgementMap() {
        return replicaAcknowledgementMap;
    }

    public AcknowledgementData getAcknowledgementDataByServerName(String nodeName) {
        return replicaAcknowledgementMap.get(nodeName);
    }

    public List<String> getAcknowledgedListForTimeStamp() {
        List<String> list = new ArrayList<>();
        for (String name : replicaAcknowledgementMap.keySet()) {
            AcknowledgementData data = replicaAcknowledgementMap.get(name);
            if (data.isAcknowledge())
                list.add(name);
        }
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
}


