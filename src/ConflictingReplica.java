public class ConflictingReplica {
    private Node.WrapperMessage message;
	private ServerData nodeServerData;


	public ConflictingReplica(ServerData nodeServerDataI, Node.WrapperMessage messageI, Node.RequestType requestTypeI) {
        nodeServerData = nodeServerDataI;
        message = messageI;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ConflictingReplica that = (ConflictingReplica) o;
        return nodeServerData.equals(that.getNodeServerData());
    }

    @Override
    public int hashCode() {
        return getNodeServerData().hashCode();
    }

    public Node.WrapperMessage getMessage() {
        return message;
    }

	public ServerData getNodeServerData() {
        return nodeServerData;
    }

}
