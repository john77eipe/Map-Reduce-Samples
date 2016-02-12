package com.sample.records;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class CustomRecord implements WritableComparable<CustomRecord> {

	private Text firstName;
	private Text lastName;

	public CustomRecord() {
		set(new Text(), new Text());
	}

	public CustomRecord(String firstName, String lastName) {
		set(new Text(firstName), new Text(lastName));
	}

	public CustomRecord(Text firstName, Text lastName) {
		set(firstName, lastName);
	}

	public void set(Text firstName, Text lastName) {
		this.firstName = firstName;
		this.lastName = lastName;
	}

	public Text getfirstName() {
		return firstName;
	}

	public Text getlastName() {
		return lastName;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		firstName.write(out);
		lastName.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		firstName.readFields(in);
		lastName.readFields(in);
	}

	@Override
	public int hashCode() {
		return firstName.hashCode() * 163 + lastName.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof CustomRecord) {
			CustomRecord tp = (CustomRecord) o;
			return firstName.equals(tp.firstName) && lastName.equals(tp.lastName);
		}
		return false;
	}

	@Override
	public String toString() {
		return firstName + "\t" + lastName;
	}

	@Override
	public int compareTo(CustomRecord tp) {
		int cmp = firstName.compareTo(tp.firstName);
		if (cmp != 0) {
			return cmp;
		}
		return lastName.compareTo(tp.lastName);
	}

	//custom record comparator
	public static class Comparator extends WritableComparator {

		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

		public Comparator() {
			super(CustomRecord.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

			try {
				int firstNameL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
				int firstNameL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
				int cmp = TEXT_COMPARATOR.compare(b1, s1, firstNameL1, b2, s2, firstNameL2);
				if (cmp != 0) {
					return cmp;
				}
				return TEXT_COMPARATOR.compare(b1, s1 + firstNameL1, l1 - firstNameL1, b2, s2 + firstNameL2,
						l2 - firstNameL2);
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}
	}

	//setting a default comparator
	static {
		WritableComparator.define(CustomRecord.class, new Comparator());
	}

	//another custom record comparator
	public static class firstNameComparator extends WritableComparator {

		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

		public firstNameComparator() {
			super(CustomRecord.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

			try {
				int firstNameL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
				int firstNameL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
				return TEXT_COMPARATOR.compare(b1, s1, firstNameL1, b2, s2, firstNameL2);
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			if (a instanceof CustomRecord && b instanceof CustomRecord) {
				return ((CustomRecord) a).firstName.compareTo(((CustomRecord) b).firstName);
			}
			return super.compare(a, b);
		}
	}
}