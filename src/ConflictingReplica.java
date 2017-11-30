public class ConflictingReplica {
    private Node.WrapperMessage message;
	private String serverName;

	public ConflictingReplica(String serverNameI, Node.WrapperMessage messageI) {
		serverName = serverNameI;
        message = messageI;
    }
	
    public Node.WrapperMessage getMessage() {
        return message;
    }

	public String getServerName() {
		return serverName;
    }

}
