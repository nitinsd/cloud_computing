package edu.bu.metcs.hw2.task3;

public class DriverMPMTuple {
	String driver;
	double mpm;
	
	public DriverMPMTuple(String driver, double mpm) {
		this.driver = driver;
		this.mpm = mpm;
	}
	
	public void setDriver(String driver) {
		this.driver = driver;
	}
	public void setMpm(double mpm) {
		this.mpm = mpm;
	}

	public String getDriver() {
		return driver;
	}
	public double getMpm() {
		return mpm;
	}
}
