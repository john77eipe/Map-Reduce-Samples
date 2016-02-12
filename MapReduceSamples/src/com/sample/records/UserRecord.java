package com.sample.records;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class UserRecord  implements Writable{
	
	private Text firstName;
	private Text lastName;
	private IntWritable age;

	public UserRecord(String firstName, String lastName, Integer age ) {
		this.firstName = new Text(firstName);
		this.lastName = new Text(lastName);
		this.age = new IntWritable(age);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// It is used to deserialize the fields of the object from ‘in’.
		firstName.readFields(in);
		lastName.readFields(in);
		age.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// It is used to serialize the fields of the object to ‘out’.
		firstName.write(out);
		lastName.write(out);
		age.write(out);
	}

	public Text getFirstName() {
		return firstName;
	}

	public void setFirstName(Text firstName) {
		this.firstName = firstName;
	}

	public Text getLastName() {
		return lastName;
	}

	public void setLastName(Text lastName) {
		this.lastName = lastName;
	}

	public IntWritable getAge() {
		return age;
	}

	public void setAge(IntWritable age) {
		this.age = age;
	}

	
}
