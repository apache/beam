package org.apache.beam.sdk.io.hadoop.inputformat.custom;

import java.io.Serializable;

public class MyCassandraRow implements Serializable{
	private String subscriberEmail;
	public MyCassandraRow(String email) {
		this.subscriberEmail = email;
	}

	public String getSubscriberEmail() {
		return subscriberEmail;
	}

	public void setSubscriberEmail(String subscriberEmail) {
		this.subscriberEmail = subscriberEmail;
	}

}
