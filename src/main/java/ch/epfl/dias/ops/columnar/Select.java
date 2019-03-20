package ch.epfl.dias.ops.columnar;

import java.util.ArrayList;
import java.util.List;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.column.DBColumn;

public class Select implements ColumnarOperator {

	// TODO: Add required structures
	public ColumnarOperator child;
	public BinaryOp op;
	public int fieldNo;
	public int value;

	public Select(ColumnarOperator child, BinaryOp op, int fieldNo, int value) {
		// TODO: Implement
		this.child = child;
		this.op = op;
		this.fieldNo = fieldNo;
		this.value = value;
	}

	@Override
	public DBColumn[] execute() {
		// TODO: Implement
		//get the whole table through scan
		DBColumn[] wholeTable = child.execute();
		
		//get the specific column as Integer Array
		Integer[] filterColumn = wholeTable[fieldNo].getAsInteger();
		
		//selected index
		List<Integer> selectedIndex = new ArrayList<>();
		for(int i = 0; i < filterColumn.length; i++) {
			boolean compareResult = compare(filterColumn[i]);
			if(compareResult) {
				selectedIndex.add(i);
			}
		}
		
		//materialize
		DBColumn[] newWholeTable = new DBColumn[wholeTable.length];
		int index = 0;
		for(DBColumn column : wholeTable) {
			List<Object> newColumn = new ArrayList<>();
			for(int i = 0; i < selectedIndex.size(); i++) {
				newColumn.add(column.fields[selectedIndex.get(i)]);
			}
			newWholeTable[index] = new DBColumn(newColumn.toArray(), column.type);
			index++;
		}
		
		return newWholeTable;
	}
	
	private boolean compare(Integer columnValue) {
		switch(op) {
		case LT:
			return (columnValue < value) ? true : false;
		case LE:
			return (columnValue <= value) ? true : false;
		case EQ:
			return (columnValue == value) ? true : false;
		case NE:
			return (columnValue != value) ? true : false;
		case GT:
			return (columnValue > value) ? true : false;
		case GE:
			return (columnValue >= value) ? true : false;
		default:
			return false;
		}
	}
}
