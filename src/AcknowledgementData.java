public class AcknowledgementData {
    private String key;
    private String value;
    private boolean acknowledge;

    public AcknowledgementData(String keyI,String valueI){
        key =keyI;
        value = valueI;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public boolean isAcknowledge() {
        return acknowledge;
    }

    public void setAcknowledge(boolean acknowledge) {
        this.acknowledge = acknowledge;
    }
}
