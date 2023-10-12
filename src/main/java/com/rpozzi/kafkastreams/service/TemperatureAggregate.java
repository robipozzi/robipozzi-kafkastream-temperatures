package com.rpozzi.kafkastreams.service;

import java.io.Serializable;

public class TemperatureAggregate implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private int sum;
    private int count;

    public TemperatureAggregate(int sum, int count) {
        this.sum = sum;
        this.count = count;
    }

    public int getSum() {
        return sum;
    }

    public int getCount() {
        return count;
    }

    public TemperatureAggregate add(int temperature) {
        sum += temperature;
        count++;
        return this;
    }

    public double getAverage() {
        if (count == 0) {
            return 0.0; // Avoid division by zero
        }
        return sum / count;
    }
}