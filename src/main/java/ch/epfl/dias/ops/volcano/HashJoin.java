package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class HashJoin implements VolcanoOperator {

	// TODO: Add required structures
	private VolcanoOperator leftChild;
	private VolcanoOperator rightChild;
	private int leftFieldNo;
	private int rightFieldNo;
	
	private HashMap<Integer, List<DBTuple>> hashMap = new HashMap<Integer, List<DBTuple>>();
	private List<DBTuple> selectedLeftTuples = new ArrayList<DBTuple>();
	private Iterator<DBTuple> iterator;
	private DBTuple leftTuple;
	private DBTuple rightTuple; 
	
	public HashJoin(VolcanoOperator leftChild, VolcanoOperator rightChild, int leftFieldNo, int rightFieldNo) {
		// TODO: Implement
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;
	}

	@Override
	public void open() {
		// TODO: Implement
		leftChild.open();
		rightChild.open();
		hashMap.clear();
		
		//Create HashMap of the leftChild
		createHashMap();
		
		//initialize
		rightTuple = rightChild.next();
		Integer rightFieldValue = rightTuple.getFieldAsInt(rightFieldNo);
		selectedLeftTuples = hashMap.get(rightFieldValue);
		if(selectedLeftTuples == null) {
			iterator = Collections.emptyIterator();
		}else {
			iterator = selectedLeftTuples.iterator();
		}
	}

	@Override
	public DBTuple next() {
		// TODO: Implement
		while(!rightTuple.eof) {
			if(iterator.hasNext()) {
				leftTuple = iterator.next();
				return join();
			}else {
				rightTuple = rightChild.next();
				if(!rightTuple.eof) {
					Integer rightFieldValue = rightTuple.getFieldAsInt(rightFieldNo);
					selectedLeftTuples = hashMap.get(rightFieldValue);
					if(selectedLeftTuples == null) {
						iterator = Collections.emptyIterator();
					}else {
						iterator = selectedLeftTuples.iterator();
					}
				}
			}
		}
		return rightTuple;
	}

	@Override
	public void close() {
		// TODO: Implement
		leftChild.close();
		rightChild.close();
		hashMap.clear();
	}
	
	private void createHashMap() {
		DBTuple tuple = leftChild.next();
		while(!tuple.eof) {
			Integer leftFieldValue = tuple.getFieldAsInt(leftFieldNo);
			if(hashMap.get(leftFieldValue) == null) {
				hashMap.put(leftFieldValue, new ArrayList<DBTuple>());	
			}
			hashMap.get(leftFieldValue).add(tuple);
			tuple = leftChild.next();
		}
	}
	
	private DBTuple join() {
		int leftLength = leftTuple.fields.length;
		int rightLength = rightTuple.fields.length;
		int length = leftLength + rightLength;
		
		Object[] newFields = new Object[length];
		DataType[] newTypes = new DataType[length];
		
		int idx = 0;
		for(int i = 0; i < leftLength; i++) {
			newFields[idx] = leftTuple.fields[i];
			newTypes[idx] = leftTuple.types[i];
			idx++;
		}
		for(int i = 0; i < rightLength; i++) {
			newFields[idx] = rightTuple.fields[i];
			newTypes[idx] = rightTuple.types[i];
			idx++;
		}
		
		return new DBTuple(newFields, newTypes);
	}
}
