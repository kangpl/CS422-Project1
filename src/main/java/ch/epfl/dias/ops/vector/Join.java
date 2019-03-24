package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class Join implements VectorOperator {

	// TODO: Add required structures
	public VectorOperator leftChild;
	public VectorOperator rightChild;
	public int leftFieldNo;
	public int rightFieldNo;
	
	//information of the left Table
	public HashMap<Integer, List<Object[]>> hashMap = new HashMap<>();
	public DataType[] leftTypes;
	
	//information of the right Table
	public DBColumn[] rightVector;
	public int rightVectorSize;
	public DataType[] rightTypes;
	public boolean newRightVector;
	
	//information of the joinFields and output Table
	public int indexNum;
	public List<Object[]> joinFields = new ArrayList<>();
	public DBColumn[] outputTable;
	public DataType[] outputTypes;

	public Join(VectorOperator leftChild, VectorOperator rightChild, int leftFieldNo, int rightFieldNo) {
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
		indexNum = 0;
		rightVector = rightChild.next();
		rightVectorSize = rightVector[0].fields.length;
		rightTypes = new DataType[rightVector.length];
		for(int i = 0; i < rightVector.length; i++) {
			rightTypes[i] = rightVector[i].type;
		}
		
		outputTypes = new DataType[leftTypes.length + rightTypes.length];
		for(int i = 0; i < leftTypes.length; i++) {
			outputTypes[i] = leftTypes[i];
		}
		for(int i = 0; i < rightTypes.length; i++) {
			outputTypes[leftTypes.length + i] = rightTypes[i];
		}
		this.newRightVector = true;
	}

	@Override
	public DBColumn[] next() {
		// TODO: Implement
		while(rightVector[0].fields.length != 0) {
			if(this.newRightVector) {
				Integer[] rightFilterColumn = rightVector[rightFieldNo].getAsInteger();
				
				for(int i = 0; i < rightFilterColumn.length; i++) {
					List<Object[]> leftMatching = hashMap.get(rightFilterColumn[i]);
					if(leftMatching != null) {
						for(int j = 0; j < leftMatching.size(); j++) {
							Object[] leftFields = leftMatching.get(j);
							Object[] fields = new Object[leftTypes.length + rightVector.length];
							for(int index = 0; index < leftTypes.length; index ++) {
								fields[index] = leftFields[index];
							}
							for(int index = 0; index < rightVector.length; index++) {
								fields[leftTypes.length + index] = rightVector[index].fields[i];
							}
							joinFields.add(fields);
						}
					}
				}
				this.newRightVector = false;
			}
			
			if(indexNum + rightVectorSize <= joinFields.size()) {
				outputTable = new DBColumn[leftTypes.length + rightTypes.length];
				for(int i = 0; i < outputTable.length; i++) {
					Object[] columnData = new Object[rightVectorSize];
					for(int j = 0; j < rightVectorSize; j++) {
						columnData[j] = joinFields.get(indexNum + j)[i];
					}
					outputTable[i] = new DBColumn(columnData, outputTypes[i]);
				}
				indexNum += rightVectorSize;
				return outputTable;
			}
			
			else {
				rightVector = rightChild.next();
				this.newRightVector = true;
			}
		}
		if(indexNum < joinFields.size()) {
			outputTable = new DBColumn[leftTypes.length + rightTypes.length];
			for(int i = 0; i < outputTable.length; i++) {
				Object[] columnData = new Object[joinFields.size() - indexNum];
				for(int j = 0; j < columnData.length; j++) {
					columnData[j] = joinFields.get(indexNum + j)[i];
				}
				outputTable[i] = new DBColumn(columnData, outputTypes[i]);
			}
			indexNum += rightVectorSize;
			return outputTable;
			
		} else {
			return new DBColumn[] {new DBColumn(new Object[] {}, DataType.INT)};
		}
	}

	@Override
	public void close() {
		// TODO: Implement
		leftChild.close();
		rightChild.close();
		indexNum = 0;
		hashMap.clear();
	}
	
	private void createHashMap(){
		DBColumn[] leftVector = leftChild.next();
		
		leftTypes = new DataType[leftVector.length];
		for(int i = 0; i < leftVector.length; i++) {
			leftTypes[i] = leftVector[i].type;
		}
		
		while(leftVector[0].fields.length != 0) {
			Integer[] leftFieldValue = leftVector[leftFieldNo].getAsInteger();
			
			for(int i = 0; i < leftFieldValue.length; i++) {
				Object[] fields = new Object[leftVector.length];
				for(int j = 0; j < leftVector.length; j++) {
					fields[j] = leftVector[j].fields[i];
				}
				
				int val = leftFieldValue[i];
				if(hashMap.get(val) == null) {
					hashMap.put(val, new ArrayList<Object[]>());	
				}
				hashMap.get(val).add(fields);
			}
			leftVector = leftChild.next();
		}		
	}
}
