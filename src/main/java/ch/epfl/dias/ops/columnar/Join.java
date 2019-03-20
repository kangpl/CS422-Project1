package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
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
		
		//get the specific column as Integer Array
		Integer[] leftFilterColumn = leftTable[leftFieldNo].getAsInteger();
		Integer[] rightFilterColumn = rightTable[rightFieldNo].getAsInteger();
		
		List<Integer[]> selectedIndexPair = new ArrayList<>();
		for(int i = 0; i < leftFilterColumn.length; i++) {
			int leftValue = leftFilterColumn[i];
			for(int j = 0; j < rightFilterColumn.length; j++) {
				int rightValue = rightFilterColumn[j];
				if(leftValue == rightValue) {
					Integer[] indexPair = {i, j};
					selectedIndexPair.add(indexPair);
				}
				
			}
		}
		
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
}
