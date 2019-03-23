package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.column.DBColumnId;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class Join implements ColumnarOperator {

	// TODO: Add required structures
	public ColumnarOperator leftChild;
	public ColumnarOperator rightChild;
	public int leftFieldNo;
	public int rightFieldNo;

	public Join(ColumnarOperator leftChild, ColumnarOperator rightChild, int leftFieldNo, int rightFieldNo) {
		// TODO: Implement
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;
	}

	public DBColumn[] execute() {
		// TODO: Implement
		DBColumn[] leftTable = leftChild.execute();
		DBColumn[] rightTable = rightChild.execute();
		
		//Create HashTable of left table
		HashMap<Integer, List<Integer>> leftHashTable= new HashMap<>();
		Integer[] leftFilterColumn = leftTable[leftFieldNo].getAsInteger();
		for(int i = 0; i < leftFilterColumn.length; i++) {
			if(leftHashTable.get(leftFilterColumn[i]) == null) {
				leftHashTable.put(leftFilterColumn[i], new ArrayList<Integer>());	
			}
			leftHashTable.get(leftFilterColumn[i]).add(i);
		}

		List<Integer[]> selectedIndexPair = new ArrayList<>();
		Integer[] rightFilterColumn = rightTable[rightFieldNo].getAsInteger();
		
		for(int i = 0; i < rightFilterColumn.length; i++) {
			List<Integer> leftMatchingIndex = leftHashTable.get(rightFilterColumn[i]);
			if(leftMatchingIndex != null) {
				for(int j = 0; j < leftMatchingIndex.size(); j++) {
					Integer[] indexPair = {leftMatchingIndex.get(j), i};
					selectedIndexPair.add(indexPair);
				}
			}
		}
		
		//check about materialization
		if(leftTable[0].lateMaterialization == false && rightTable[0].lateMaterialization == false){
			//early materialization
			List<DBColumn> joinTable = new ArrayList<DBColumn>();
			for(int i = 0; i < leftTable.length; i++) {
				Object[] oldColumn = leftTable[i].fields;
				Object[] newColumn = new Object[selectedIndexPair.size()];
				for(int j = 0; j < selectedIndexPair.size(); j++) {
					newColumn[j] = oldColumn[selectedIndexPair.get(j)[0]];
				}
				joinTable.add(new DBColumn(newColumn, leftTable[i].type));
				
			}
			for(int i = 0; i < rightTable.length; i++) {
				Object[] oldColumn = rightTable[i].fields;
				Object[] newColumn = new Object[selectedIndexPair.size()];
				for(int j = 0; j < selectedIndexPair.size(); j++) {
					newColumn[j] = oldColumn[selectedIndexPair.get(j)[1]];
				}
				joinTable.add(new DBColumn(newColumn, rightTable[i].type));
			}			
			
	 		return joinTable.toArray(new DBColumn[0]);
		}
		else {
			//late materialization update ids
			DBColumnId[] lateLeftTable = (DBColumnId[]) leftTable;
			DBColumnId[] lateRightTable = (DBColumnId[]) rightTable;
			
			int leftColumnNum = leftTable.length;
			int rightColumnNum = rightTable.length;
			int attributesLength = selectedIndexPair.size();
			//update ids
			//attributes length
			int[] newLeftIds = new int[attributesLength];
			int[] newRightIds = new int[attributesLength];
			for(int i = 0; i < attributesLength; i++) {
				newLeftIds[i] = selectedIndexPair.get(i)[0];
				newRightIds[i] = selectedIndexPair.get(i)[1];
			}
			DBColumnId[] joinTable = new DBColumnId[leftColumnNum + rightColumnNum];
			
			for(int i = 0; i< leftColumnNum; i++) {
				DBColumnId oldColumn = lateLeftTable[i];
				joinTable[i] = new DBColumnId(oldColumn.fields, oldColumn.type, oldColumn.lateMaterialization, newLeftIds);
			}
			
			for(int i = 0; i< rightColumnNum; i++) {
				DBColumnId oldColumn = lateRightTable[i];
				joinTable[leftColumnNum + i] = new DBColumnId(oldColumn.fields, oldColumn.type, oldColumn.lateMaterialization, newRightIds);
			}
			return joinTable;		
		}		
	}
}
