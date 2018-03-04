package edu.bu.metcs.hw2.task2;

public class MedallionErrorRateTuple {
	String medallion;
	float errorRate;
	
	public MedallionErrorRateTuple(String medallion, float errorRate) {
		this.medallion = medallion;
		this.errorRate = errorRate;
	}
	
	public void setHourOfDay(String medallion) {
		this.medallion = medallion;
	}
	public void setErrorRate(float errorRate) {
		this.errorRate = errorRate;
	}

	public String getMedallion() {
		return medallion;
	}
	public float getErrorRate() {
		return errorRate;
	}
}
