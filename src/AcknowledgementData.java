public class AcknowledgementData implements Comparable {
    private int key;
    private String value;
    private boolean acknowledge;
    private String timeStamp;
    private String replicaName;

    public AcknowledgementData(int keyI, String valueI, String timeStampI,String replicaNameI) {
        key = keyI;
        value = valueI;
        timeStamp = timeStampI;
        replicaName = replicaNameI;
    }

    public int getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStampI) {
        timeStamp = timeStampI;
    }

    public boolean isAcknowledge() {
        return acknowledge;
    }

    public void setAcknowledge(boolean acknowledge) {
        this.acknowledge = acknowledge;
    }

    public void setValue(String valueI) {
        value = valueI;
    }


    @Override
    public int compareTo(Object o) {
        return ((AcknowledgementData) o).timeStamp.compareTo(timeStamp);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AcknowledgementData) {
            return ((AcknowledgementData) obj).timeStamp == timeStamp;
        }
        return false;
    }

    public String getReplicaName() {
        return replicaName;
    }
}
