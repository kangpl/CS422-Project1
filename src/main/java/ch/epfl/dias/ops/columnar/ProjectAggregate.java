package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.column.DBColumnId;

public class ProjectAggregate implements ColumnarOperator {

	// TODO: Add required structures
	public ColumnarOperator child;
	public Aggregate agg;
	public DataType dt;
	public int fieldNo;
	
	public ProjectAggregate(ColumnarOperator child, Aggregate agg, DataType dt, int fieldNo) {
		// TODO: Implement
		this.child = child;
		this.agg = agg;
		this.dt = dt;
		this.fieldNo = fieldNo;
	}

	@Override
	public DBColumn[] execute() {
		// TODO: Implement
		DBColumn[] wholeTable = child.execute();
		
		if(wholeTable[0].lateMaterialization == false) {
			DBColumn column = wholeTable[fieldNo];
			DBColumn[] aggResult = {null};
			
			switch(agg) {
			case COUNT:
				Object[] count = {column.fields.length};
				aggResult[0] = new DBColumn(count, DataType.INT);
				return aggResult;
			case SUM:
				switch(dt) {
				case INT:
					Integer[] intColumn = column.getAsInteger();
					int intSum = 0;
					for(int elem : intColumn) { intSum += elem;}
					Object[] intSumToObject = {intSum};
					aggResult[0] = new DBColumn(intSumToObject, DataType.INT);
					return aggResult;
				case DOUBLE:
					Double[] doubleColumn = column.getAsDouble();
					int doubleSum = 0;
					for(double elem : doubleColumn) { doubleSum += elem; }
					Object[] doubleSumToObject = {doubleSum};
					aggResult[0] = new DBColumn(doubleSumToObject, DataType.DOUBLE);
					return aggResult;
				}
			case MIN:
				switch(dt) {
				case INT:
					Integer[] intColumn = column.getAsInteger();
					int intMin = Integer.MAX_VALUE;
					for(int elem : intColumn) { intMin = elem < intMin ? elem : intMin; }
					Object[] intMinToObject = {intMin};
					aggResult[0] = new DBColumn(intMinToObject, DataType.INT);
					return aggResult;
				case DOUBLE:
					Double[] doubleColumn = column.getAsDouble();
					double doubleMin = Double.MAX_VALUE;
					for(double elem : doubleColumn) { doubleMin = elem < doubleMin ? elem : doubleMin; }
					Object[] doubleMinToObject = {doubleMin};
					aggResult[0] = new DBColumn(doubleMinToObject, DataType.DOUBLE);
					return aggResult;
				}
			case MAX:
				switch(dt) {
				case INT:
					Integer[] intColumn = column.getAsInteger();
					int intMax = Integer.MIN_VALUE;
					for(int elem : intColumn) { intMax = elem > intMax ? elem : intMax; }
					Object[] intMaxToObject = {intMax};
					aggResult[0] = new DBColumn(intMaxToObject, DataType.INT);
					return aggResult;
				case DOUBLE:
					Double[] doubleColumn = column.getAsDouble();
					double doubleMax = Double.MIN_VALUE;
					for(double elem : doubleColumn) { doubleMax = elem > doubleMax ? elem : doubleMax; }
					Object[] doubleMaxToObject = {doubleMax};
					aggResult[0] = new DBColumn(doubleMaxToObject, DataType.DOUBLE);
					return aggResult;
				}
			case AVG:
				switch(dt) {
				case INT:
					Integer[] intColumn = column.getAsInteger();
					int intAvgSum = 0;
					for(int elem : intColumn) { intAvgSum += elem;}
					double intAvg = (double) intAvgSum / (double) intColumn.length;
					
					Object[] intAvgToObject = {intAvg};
					aggResult[0] = new DBColumn(intAvgToObject, DataType.DOUBLE);
					return aggResult;
					
				case DOUBLE:
					Double[] doubleColumn = column.getAsDouble();
					double doubleAvgSum = 0.0;
					for(double elem : doubleColumn) {doubleAvgSum += elem;}
					double doubleAvg = doubleAvgSum / doubleColumn.length;
					
					Object[] doubleAvgToObject = {doubleAvg};
					aggResult[0] = new DBColumn(doubleAvgToObject, DataType.DOUBLE);
					return aggResult;			
				}			
			}
		}
		else {
			DBColumnId column = (DBColumnId)wholeTable[fieldNo];
			DBColumnId[] aggResult = {null};
			int[] index = {0};
			
			switch(agg) {
			case COUNT:
				Object[] count = {column.ids.length};
				aggResult[0] = new DBColumnId(count, DataType.INT, true, index);
				return aggResult;
			case SUM:
				switch(dt) {
				case INT:
					Integer[] intColumn = column.getAsInteger();
					int intSum = 0;
					for(int elem : intColumn) { intSum += elem;}
					Object[] intSumToObject = {intSum};
					aggResult[0] = new DBColumnId(intSumToObject, DataType.INT, true, index);
					return aggResult;
				case DOUBLE:
					Double[] doubleColumn = column.getAsDouble();
					int doubleSum = 0;
					for(double elem : doubleColumn) { doubleSum += elem; }
					Object[] doubleSumToObject = {doubleSum};
					aggResult[0] = new DBColumnId(doubleSumToObject, DataType.DOUBLE, true, index);
					return aggResult;
				}
			case MIN:
				switch(dt) {
				case INT:
					Integer[] intColumn = column.getAsInteger();
					int intMin = Integer.MAX_VALUE;
					for(int elem : intColumn) { intMin = elem < intMin ? elem : intMin; }
					Object[] intMinToObject = {intMin};
					aggResult[0] = new DBColumnId(intMinToObject, DataType.INT, true, index);
					return aggResult;
				case DOUBLE:
					Double[] doubleColumn = column.getAsDouble();
					double doubleMin = Double.MAX_VALUE;
					for(double elem : doubleColumn) { doubleMin = elem < doubleMin ? elem : doubleMin; }
					Object[] doubleMinToObject = {doubleMin};
					aggResult[0] = new DBColumnId(doubleMinToObject, DataType.DOUBLE, true, index);
					return aggResult;
				}
			case MAX:
				switch(dt) {
				case INT:
					Integer[] intColumn = column.getAsInteger();
					int intMax = Integer.MIN_VALUE;
					for(int elem : intColumn) { intMax = elem > intMax ? elem : intMax; }
					Object[] intMaxToObject = {intMax};
					aggResult[0] = new DBColumnId(intMaxToObject, DataType.INT, true, index);
					return aggResult;
				case DOUBLE:
					Double[] doubleColumn = column.getAsDouble();
					double doubleMax = Double.MIN_VALUE;
					for(double elem : doubleColumn) { doubleMax = elem > doubleMax ? elem : doubleMax; }
					Object[] doubleMaxToObject = {doubleMax};
					aggResult[0] = new DBColumnId(doubleMaxToObject, DataType.DOUBLE, true, index);
					return aggResult;
				}
			case AVG:
				switch(dt) {
				case INT:
					Integer[] intColumn = column.getAsInteger();
					int intAvgSum = 0;
					for(int elem : intColumn) { intAvgSum += elem;}
					double intAvg = (double) intAvgSum / (double) intColumn.length;
					
					Object[] intAvgToObject = {intAvg};
					aggResult[0] = new DBColumnId(intAvgToObject, DataType.DOUBLE, true, index);
					return aggResult;
					
				case DOUBLE:
					Double[] doubleColumn = column.getAsDouble();
					double doubleAvgSum = 0.0;
					for(double elem : doubleColumn) {doubleAvgSum += elem;}
					double doubleAvg = doubleAvgSum / doubleColumn.length;
					
					Object[] doubleAvgToObject = {doubleAvg};
					aggResult[0] = new DBColumnId(doubleAvgToObject, DataType.DOUBLE, true, index);
					return aggResult;			
				}			
			}		
		}
		
		return null;
	}
}
