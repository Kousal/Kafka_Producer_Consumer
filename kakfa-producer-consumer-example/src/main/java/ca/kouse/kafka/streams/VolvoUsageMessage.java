package ca.kouse.kafka.streams;

public class VolvoUsageMessage {
	private String lob;
	private String appName;
	private String appId;
	private int numberOfUsers;
	private int numberOfDevelopers;
	private int count;
	public String getLob() {
		return lob;
	}
	public void setLob(String lob) {
		this.lob = lob;
	}
	public String getAppName() {
		return appName;
	}
	public void setAppName(String appName) {
		this.appName = appName;
	}
	public String getAppId() {
		return appId;
	}
	public void setAppId(String appId) {
		this.appId = appId;
	}
	public int getNumberOfUsers() {
		return numberOfUsers;
	}
	public void setNumberOfUsers(int numberOfUsers) {
		this.numberOfUsers = numberOfUsers;
	}
	public int getNumberOfDevelopers() {
		return numberOfDevelopers;
	}
	public void setNumberOfDevelopers(int numberOfDevelopers) {
		this.numberOfDevelopers = numberOfDevelopers;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	
}
