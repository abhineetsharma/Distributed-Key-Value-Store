public class NodeServerData {
    private String name;
    private String ip;
    private int port;

    public NodeServerData(String str) {
        String[] sarr = str.split(" ");
        if (sarr.length == 3) {
            name = sarr[0];
            ip = sarr[1];
            port = Integer.parseInt(sarr[2]);
        }

    }

    public String getName() {
        return name;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return "[Name : " + name + ", IP : " + ip + ", Port : " + port + "]";
    }
}
