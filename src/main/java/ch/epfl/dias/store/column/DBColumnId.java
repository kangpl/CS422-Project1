package ch.epfl.dias.store.column;

import ch.epfl.dias.store.DataType;

public class DBColumnId extends DBColumn {

	public int[] ids;

	public DBColumnId(Object[] fields, DataType type, boolean lateMaterialization, int[] ids) {
		super(fields, type, lateMaterialization);
		// TODO Auto-generated constructor stub
		this.ids = ids;	
	}
	
	@Override
	public Integer[] getAsInteger() {
		// TODO: Implement
		Integer[] integerArray = new Integer[ids.length];
		
		for(int i = 0; i < ids.length; i++) {
			integerArray[i] = (Integer)fields[ids[i]];	
		}
		return integerArray;
	}
	
	@Override
	public String[] getAsString() {
		// TODO: Implement
		String[] stringArray = new String[ids.length];
		
		for(int i = 0; i < ids.length; i++) {
			stringArray[i] = (String)fields[ids[i]];	
		}
		return stringArray;
	}
	
	@Override
	public Boolean[] getAsBoolean() {
		// TODO: Implement
		Boolean[] booleanArray = new Boolean[ids.length];
		
		for(int i = 0; i < ids.length; i++) {
			booleanArray[i] = (Boolean)fields[ids[i]];	
		}
		return booleanArray;
	}
	
	@Override
	public Double[] getAsDouble() {
		// TODO: Implement
		Double[] doubleArray = new Double[ids.length];
		
		for(int i = 0; i < ids.length; i++) {
			doubleArray[i] = (Double)fields[ids[i]];	
		}
		return doubleArray;
	}


}
