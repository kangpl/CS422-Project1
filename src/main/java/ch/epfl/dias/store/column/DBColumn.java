package ch.epfl.dias.store.column;

import java.util.ArrayList;
import java.util.Arrays;

import ch.epfl.dias.store.DataType;

public class DBColumn {

	// TODO: Implement
	public Object[] fields;
	public DataType type;

	public DBColumn(Object[] fields, DataType type) {
		this.fields = fields;
		this.type = type;
	}
	
	public Integer[] getAsInteger() {
		// TODO: Implement
		Integer[] integerArray = new Integer[fields.length];
		
		for(int i = 0; i < fields.length; i++) {
			integerArray[i] = (Integer)fields[i];
		}
		return integerArray;
	}
	
	public String[] getAsString() {
		// TODO: Implement
		String[] stringArray = new String[fields.length];
		
		for(int i = 0; i < fields.length; i++) {
			stringArray[i] = (String)fields[i];
		}
		return stringArray;
	}
	
	public Boolean[] getAsBoolean() {
		// TODO: Implement
		Boolean[] booleanArray = new Boolean[fields.length];
		
		for(int i = 0; i < fields.length; i++) {
			booleanArray[i] = (Boolean)fields[i];
		}
		return booleanArray;
	}
	
	public Double[] getAsDouble() {
		// TODO: Implement
		Double[] doubleArray = new Double[fields.length];
		
		for(int i = 0; i < fields.length; i++) {
			doubleArray[i] = (Double)fields[i];
		}
		return doubleArray;
	}

}
