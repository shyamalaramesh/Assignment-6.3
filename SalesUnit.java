package mapreduce.task10;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class SalesUnit implements WritableComparable<SalesUnit> {

	private String state;
	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public int getPrice() {
		return price;
	}

	public void setPrice(int price) {
		this.price = price;
	}



	private int price;
	
	public void set(String state, int price) {
		this.state = state;
		this.price = price;
	}
	
	/**
	 * Used when deserializing objects. Please note that deserialization must be 
	 * done in the same order as that of serialization 
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		state = in.readUTF();
		price = in.readInt();
	}

	/**
	 * Used when serializing objects
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(state);
		out.writeInt(price);
	}
	
	/**
	 * What should appear as output in the file for this WritableCOmparable 
	 */
	public String toString() {
		return state+"\t"+price;
	}

	/**
	 * How to compare two SalesUnit object. Here we are looking for top 3 states for each company.
	 * So the descending sort will be done on  the price
	 */
	@Override
	public int compareTo(SalesUnit otherSalesUnit) {
		return otherSalesUnit.getPrice() - price;
	}

}
