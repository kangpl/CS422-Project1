package ch.epfl.dias.ops.vector;

import java.util.ArrayList;
import java.util.List;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;

public class Select implements VectorOperator {

	// TODO: Add required structures
	public VectorOperator child;
	public BinaryOp op;
	public int fieldNo;
	public int value;
	
	//the information of the vector
	public DBColumn[] vectorColumn;
	public int vectorSize;
	public DataType[] types;
	
	//the information of the selected index and fields
	public int selectedNum;
	public List<Integer> selectedIndex = new ArrayList<>();
	public List<List<Object>> selectedFields;

	//the information of the output vector
	public boolean outputFlag;
	public DBColumn[] newVectorColumn;


	public Select(VectorOperator child, BinaryOp op, int fieldNo, int value) {
		// TODO: Implement
		this.child = child;
		this.op = op;
		this.fieldNo = fieldNo;
		this.value = value;
	}
	
	@Override
	public void open() {
		// TODO: Implement
		child.open();
		
		vectorColumn = child.next();
		vectorSize = vectorColumn[0].fields.length;
		types = new DataType[vectorColumn.length];
		
		selectedNum = 0;
		selectedFields = new ArrayList<>();
		for(int j = 0; j < vectorColumn.length; j++) {
			selectedFields.add(new ArrayList<>());
			types[j] = vectorColumn[j].type;
		}
		
		this.outputFlag = false;
	}

	@Override
	public DBColumn[] next() {
		// TODO: Implement
		while(vectorSize != 0) {
			Integer[] columnData = vectorColumn[fieldNo].getAsInteger();
			for(int i = 0; i < columnData.length; i++) {
				int val = columnData[i];
				// compare val with value to get selected index
				compare(val, i);
				if(selectedNum == vectorSize) {
					//put select data into selectedFields
					getSelectedFields();
					
					//create DBColumn[] to return
					newVectorColumn = getNewVectorColumn();
					this.outputFlag = true;
					
					//re-initialize
					selectedNum = 0;
					selectedIndex.clear();
					selectedFields.clear();
					for(int j = 0; j < vectorColumn.length; j++) {
						selectedFields.add(new ArrayList<>());
					}
				}		
			}
			getSelectedFields();
			selectedIndex.clear();
			vectorColumn = child.next();
			vectorSize = vectorColumn[0].fields.length;
			if(this.outputFlag) {
				this.outputFlag = false;
				return newVectorColumn;
			}	
		}
		if(vectorSize == 0 && selectedNum != 0) {
			selectedNum = 0;
			return getNewVectorColumn();
		}
		return new DBColumn[] {new DBColumn(new Object[] {}, DataType.INT)};
	}

	@Override
	public void close() {
		// TODO: Implement
		child.close();
	}
	
	private void compare(int val, int index) {
		switch(op) {
		case LT:
			if(val < value) {
				selectedIndex.add(index);
				selectedNum += 1;
			}
			break;
		case LE:
			if(val <= value) {
				selectedIndex.add(index);
				selectedNum += 1;
			}
			break;
		case EQ:
			if(val == value) {
				selectedIndex.add(index);
				selectedNum += 1;
			}
			break;
		case NE:
			if(val != value) {
				selectedIndex.add(index);
				selectedNum += 1;
			}
			break;
		case GT:
			if(val > value) {
				selectedIndex.add(index);
				selectedNum += 1;
			}
			break;
		case GE:
			if(val >= value) {
				selectedIndex.add(index);
				selectedNum += 1;					
			}
			break;
		}
	}
	
	private void getSelectedFields() {
		int index = 0;
		for(DBColumn column : vectorColumn) {
			for(int j = 0; j < selectedIndex.size(); j++) {
				selectedFields.get(index).add(column.fields[selectedIndex.get(j)]);
			}
			index++;
		}
	}
	
	private DBColumn[] getNewVectorColumn() {
		DBColumn[] newVectorColumn = new DBColumn[selectedFields.size()];
		int index = 0;
		for(List<Object> listObject : selectedFields) {
			DBColumn newColumn = new DBColumn(listObject.toArray(new Object[0]), types[index]);
			newVectorColumn[index++] = newColumn;
		}
		return newVectorColumn;
	}
}
