/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.templates.avro;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

/**
 * Example of AVRO serialization class. To configure your AVRO schema, change this class to
 * requirement schema definition
 */
@DefaultCoder(AvroCoder.class)
public class TaxiRide {
  // "ride_id":"a60ba4d8-1501-4b5b-93ee-b7864304d0e0",
  // "latitude":40.66684000000033,
  // "longitude":-73.83933000000202,
  // "timestamp":"2016-08-31T11:04:02.025396463-04:00",
  // "meter_reading":14.270274,
  // "meter_increment":0.019336415,
  // "ride_status":"enroute" / "pickup" / "dropoff"
  // "passenger_count":2
  String rideId;
  Float latitude;
  Float longitude;
  String timestamp;
  Float meterReading;
  Float meterIncrement;
  String rideStatus;
  Integer passengerCount;

  public TaxiRide(
      String rideId,
      Float latitude,
      Float longitude,
      String timestamp,
      Float meterReading,
      Float meterIncrement,
      String rideStatus,
      Integer passengerCount) {
    this.rideId = rideId;
    this.latitude = latitude;
    this.longitude = longitude;
    this.timestamp = timestamp;
    this.meterReading = meterReading;
    this.meterIncrement = meterIncrement;
    this.rideStatus = rideStatus;
    this.passengerCount = passengerCount;
  }

  public String getRideId() {
    return rideId;
  }

  public void setRideId(String rideId) {
    this.rideId = rideId;
  }

  public Float getLatitude() {
    return latitude;
  }

  public void setLatitude(Float latitude) {
    this.latitude = latitude;
  }

  public Float getLongitude() {
    return longitude;
  }

  public void setLongitude(Float longitude) {
    this.longitude = longitude;
  }

  public String getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(String timestamp) {
    this.timestamp = timestamp;
  }

  public Float getMeterReading() {
    return meterReading;
  }

  public void setMeterReading(Float meterReading) {
    this.meterReading = meterReading;
  }

  public Float getMeterIncrement() {
    return meterIncrement;
  }

  public void setMeterIncrement(Float meterIncrement) {
    this.meterIncrement = meterIncrement;
  }

  public String getRideStatus() {
    return rideStatus;
  }

  public void setRideStatus(String rideStatus) {
    this.rideStatus = rideStatus;
  }

  public Integer getPassengerCount() {
    return passengerCount;
  }

  public void setPassengerCount(Integer passengerCount) {
    this.passengerCount = passengerCount;
  }
}
