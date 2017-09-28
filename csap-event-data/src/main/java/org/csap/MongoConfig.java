package org.csap;

public class MongoConfig {

	@Override
	public String toString() {
		return "MongoConfig [hosts=" + getHosts() + ", port=" + getPort()
				+ ", user=" + user
				+ ", password= xxxx" +  ", userdb=" + userDb + "]";
	}
	private int port;
	private String hosts;
	private String user;
	private String password;
	private String userDb;
	

	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public String getUserDb() {
		return userDb;
	}
	public void setUserDb(String userDb) {
		this.userDb = userDb;
	}

	/**
	 * @return the mongoPort
	 */
	public int getPort() {
		return port;
	}

	/**
	 * @param port the mongoPort to set
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * @return the mongoHosts
	 */
	public String getHosts() {
		return hosts;
	}

	/**
	 * @param hosts the mongoHosts to set
	 */
	public void setHosts(String hosts) {
		this.hosts = hosts;
	}
	
	

}
