package org.apache.beam.sdk.io.hadoop.inputformat.integration.tests;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class DBInputWritable implements Writable, DBWritable, Serializable {
	
	private String empIdentifier;
	private String empName;
	
	// empty constructor required for coder 
	public DBInputWritable(){
		
	}
	public DBInputWritable(String empIdentifier, String empName){
		this.empIdentifier=empIdentifier;
		this.empName=empName;
	}
	public void readFields(DataInput in) throws IOException {
		empIdentifier = in.readUTF();
		empName = in.readUTF();
	}

	public void readFields(ResultSet rs) throws SQLException
	// Resultset object represents the data returned from a SQL statement
	{
		empIdentifier = rs.getString(1);
		empName = rs.getString(2);
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(empIdentifier);
		out.writeUTF(empName);
	}

	public void write(PreparedStatement ps) throws SQLException {
		ps.setString(1, empIdentifier);
		ps.setString(2, empName);
	}

	
}
