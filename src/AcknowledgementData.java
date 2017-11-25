public class AcknowledgementData {
    private int key;
    private String value;
    private boolean acknowledge;
    private String timeStamp;

    public AcknowledgementData(int keyI,String valueI,String timeStampI){
        key =keyI;
        value = valueI;
        timeStamp = timeStampI;
    }

    public int getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public String getTimeStamp(){
        return timeStamp;
    }

    public boolean isAcknowledge() {
        return acknowledge;
    }

    public void setAcknowledge(boolean acknowledge) {
        this.acknowledge = acknowledge;
    }

    public void setValue(String valueI){
        value = valueI;
    }
}
