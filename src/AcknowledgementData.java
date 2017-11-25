public class AcknowledgementData {
    private int key;
    private String value;
    private boolean acknowledge;

    public AcknowledgementData(int keyI,String valueI){
        key =keyI;
        value = valueI;
    }

    public int getKey() {
        return key;
    }



    public String getValue() {
        return value;
    }



    public boolean isAcknowledge() {
        return acknowledge;
    }

    public void setAcknowledge(boolean acknowledge) {
        this.acknowledge = acknowledge;
    }
}
