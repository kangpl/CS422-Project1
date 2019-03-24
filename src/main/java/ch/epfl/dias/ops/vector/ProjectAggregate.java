package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

public class ProjectAggregate implements VectorOperator {

	// TODO: Add required structures
	public VectorOperator child;
	public Aggregate agg;
	public DataType dt;
	public int fieldNo;
	
	public int intSum;
	public int intMin;
	public int intMax;
	public double doubleSum;
	public double doubleMin;
	public double doubleMax;
	public int count;

	public ProjectAggregate(VectorOperator child, Aggregate agg, DataType dt, int fieldNo) {
		// TODO: Implement
		this.child = child;
		this.agg = agg;
		this.dt = dt;
		this.fieldNo = fieldNo;
	}

	@Override
	public void open() {
		// TODO: Implement
		child.open();
		
		intSum = 0;
		intMin = Integer.MAX_VALUE;
		intMax = Integer.MIN_VALUE;
		doubleSum = 0.0;
		doubleMin = Double.MAX_VALUE;
		doubleMax = Double.MIN_VALUE;
		count = 0;
	}

	@Override
	public DBColumn[] next() {
		// TODO: Implement
		calAggregate();
		return getAggregate();
	}

	@Override
	public void close() {
		// TODO: Implement
		child.close();
		
		intSum = 0;
		intMin = Integer.MAX_VALUE;
		intMax = Integer.MIN_VALUE;
		doubleSum = 0.0;
		doubleMin = Double.MAX_VALUE;
		doubleMax = Double.MIN_VALUE;
		count = 0;
	}
	
	private void calAggregate() {
		DBColumn[] childVector = child.next();
		while(childVector[0].fields.length != 0) {
			switch(agg) {
			case COUNT:
				count += childVector[fieldNo].fields.length;
				break;
			case SUM:
				switch(dt) {
				case INT:
					Integer[] intValue = childVector[fieldNo].getAsInteger();
					for(int val : intValue) { intSum += val; }
					break;
				case DOUBLE:
					Double[] doubleValue = childVector[fieldNo].getAsDouble();
					for(double val : doubleValue) { doubleSum += val; }
					
					break;
				default:
					break;
				}
				break;
			case MIN:
				switch(dt) {
				case INT:
					Integer[] intValue = childVector[fieldNo].getAsInteger();
					for(int val : intValue) { intMin = val < intMin ? val : intMin;}
					break;
				case DOUBLE:
					Double[] doubleValue = childVector[fieldNo].getAsDouble();
					for(double val : doubleValue) { doubleMin = val < doubleMin ? val : doubleMin; }
					break;
				default:
					break;
				}
				break;
			case MAX:
				switch(dt) {
				case INT:
					Integer[] intValue = childVector[fieldNo].getAsInteger();
					for(int val : intValue) { intMax = val > intMax ? val : intMax;}
					break;
				case DOUBLE:
					Double[] doubleValue = childVector[fieldNo].getAsDouble();
					for(double val : doubleValue) { doubleMax = val > doubleMax ? val : doubleMax; }
					break;
				default:
					break;
				}
				break;
			case AVG:
				count += childVector[fieldNo].fields.length;
				switch(dt) {
				case INT:
					Integer[] intValue = childVector[fieldNo].getAsInteger();
					for(int val : intValue) { intSum += val; }
					break;
				case DOUBLE:
					Double[] doubleValue = childVector[fieldNo].getAsDouble();
					for(double val : doubleValue) { doubleSum += val; }
					break;
				default:
					break;
				}
				break;
			default:
				break;
			}
			childVector = child.next();
		}
	}
	
	private DBColumn[] getAggregate() {
		switch(agg) {
		case COUNT:
			return new DBColumn[] {new DBColumn(new Object[] {count}, DataType.INT)};
		case SUM:
			switch(dt) {
			case INT:
				return new DBColumn[] { new DBColumn(new Object[] {intSum}, DataType.INT)};
			case DOUBLE:
				return new DBColumn[] { new DBColumn(new Object[] {doubleSum}, DataType.DOUBLE)};
			}
		case MIN:
			switch(dt) {
			case INT:
				return new DBColumn[] { new DBColumn(new Object[] {intMin}, DataType.INT)};
			case DOUBLE:
				return new DBColumn[] { new DBColumn(new Object[] {doubleMin}, DataType.DOUBLE)};
			}
		case MAX:
			switch(dt) {
			case INT:
				return new DBColumn[] { new DBColumn(new Object[] {intMax}, DataType.INT)};
			case DOUBLE:
				return new DBColumn[] { new DBColumn(new Object[] {doubleMax}, DataType.DOUBLE)};
			}
		case AVG:
			switch(dt) {
			case INT:
				return new DBColumn[] { new DBColumn(new Object[] {(double) intSum / (double) count}, DataType.DOUBLE)};
			case DOUBLE:
				return new DBColumn[] { new DBColumn(new Object[] {(double) doubleSum / (double) count}, DataType.DOUBLE)};
			}
		default:
			return new DBColumn[] {new DBColumn(new Object[] {}, DataType.INT)};
		}	
	}
}
