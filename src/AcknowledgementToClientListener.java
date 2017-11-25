import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class AcknowledgementToClientListener {
    private String clientName;
    private OutputStream clientOutputStream;
    private Map<String, AcknowledgementData> replicaAcknowledgementMap;

    public AcknowledgementToClientListener(String clientNameI, OutputStream clientOutputStreamI, String keyI, String valueI, List<NodeServerData> nodeServerDataList) {
        clientName = clientNameI;
        clientOutputStream = clientOutputStreamI;
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
}


