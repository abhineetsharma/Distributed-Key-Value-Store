public class ConflictingReplica {
    private Node.WrapperMessage message;
	private ServerData serverData;
	private String messageType;


	public ConflictingReplica(ServerData serverDataI, Node.WrapperMessage messageI) {
		serverData = serverDataI;
        message = messageI;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ConflictingReplica that = (ConflictingReplica) o;
		return serverData.equals(that.getNodeServerData());
    }

    @Override
    public int hashCode() {
        return getNodeServerData().hashCode();
    }

    public Node.WrapperMessage getMessage() {
        return message;
    }

	public ServerData getNodeServerData() {
		return serverData;
    }

}
