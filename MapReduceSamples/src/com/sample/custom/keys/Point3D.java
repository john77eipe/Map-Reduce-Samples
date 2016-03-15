package com.sample.custom.keys;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Point3D implements WritableComparable<Point3D> {
	public float x;
	public float y;
	public float z;

	public Point3D(float x, float y, float z) {
		this.x = x;
		this.y = y;
		this.z = z;
	}

	public Point3D() {
		this(0.0f, 0.0f, 0.0f);
	}

	public void write(DataOutput out) throws IOException {
		out.writeFloat(x);
		out.writeFloat(y);
		out.writeFloat(z);
	}

	public void readFields(DataInput in) throws IOException {
		x = in.readFloat();
		y = in.readFloat();
		z = in.readFloat();
	}

	public String toString() {
		return Float.toString(x) + ", " + Float.toString(y) + ", " + Float.toString(z);
	}

	public int compareTo(Point3D other) {
		float myDistance = Point3DUtil.distanceFromOrigin(this);
		float otherDistance = Point3DUtil.distanceFromOrigin(other);
		return Float.compare(myDistance, otherDistance);
	}

	public boolean equals(Object o) {
		if (!(o instanceof Point3D)) {
			return false;
		}

		Point3D other = (Point3D) o;
		return this.x == other.x && this.y == other.y && this.z == other.z;
	}

	public int hashCode() {
		return Float.floatToIntBits(x) ^ Float.floatToIntBits(y) ^ Float.floatToIntBits(z);
	}

	static {
//		this is one way to pre-register a comparator instead of in Job class
//		WritableComparator.define(Point3D.class, new XValComparator());
	}

	public static class XValComparator extends WritableComparator {
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			float x1 = readFloat(b1, s1);
			float x2 = readFloat(b2, s2);
			return (x1 < x2) ? -1 : (x1 == x2) ? 0 : 1;
		}
	}

	public static class InverseComparator extends WritableComparator {
		@Override
		public int compare(Object a, Object b) {
			Point3D pointa = (Point3D) a;
			Point3D pointb = (Point3D) b;
			float aDistance = Point3DUtil.distanceFromOrigin(pointa);
			float bDistance = Point3DUtil.distanceFromOrigin(pointb);
			return Float.compare(bDistance, aDistance);
		}
	}

	public static class Point3DUtil {
		/** return the Euclidean distance from (0, 0, 0) */
		public static float distanceFromOrigin(Point3D point) {
			return (float) Math.sqrt(point.x * point.x + point.y * point.y + point.z * point.z);
		}
	}
}
