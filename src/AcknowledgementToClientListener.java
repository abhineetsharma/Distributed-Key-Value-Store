import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class AcknowledgementToClientListener {
    private String clientName;
    private OutputStream clientOutputStream;
    private Node.ConsistencyLevel requestConsistencyLevel;
    private boolean isSentToClinet;
    private Map<String, AcknowledgementData> replicaAcknowledgementMap;


    public AcknowledgementToClientListener(String clientNameI, OutputStream clientOutputStreamI, Node.ConsistencyLevel consistencyLevelI, int keyI, String valueI, List<NodeServerData> nodeServerDataList) {
        clientName = clientNameI;
        clientOutputStream = clientOutputStreamI;
        requestConsistencyLevel = consistencyLevelI;
        replicaAcknowledgementMap = new ConcurrentSkipListMap<>();
        for (NodeServerData node : nodeServerDataList)
            replicaAcknowledgementMap.put(node.getName(), new AcknowledgementData(keyI, valueI));
    }

    public String getClientName() {
        return clientName;
    }

    public OutputStream getClientOutputStream() {
        return clientOutputStream;
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

    public boolean isSentToClinet() {
        return isSentToClinet;
    }

    public void setSentToClinet(boolean sentToClinet) {
        isSentToClinet = sentToClinet;
    }
}


