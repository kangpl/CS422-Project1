package ch.epfl.dias;

import java.io.IOException;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.PAX.PAXStore;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;
import ch.epfl.dias.store.row.RowStore;

public class Main {

	public static void main(String[] args) throws IOException {

		DataType[] schema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT,
				DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT };

		DataType[] orderSchema = new DataType[] { DataType.INT, DataType.INT, DataType.STRING, DataType.DOUBLE,
				DataType.STRING, DataType.STRING, DataType.STRING, DataType.INT, DataType.STRING };

		schema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT,
				DataType.INT, DataType.INT, DataType.INT, DataType.INT };
//		for(DataType type : DataType.values()) {
//			System.out.println(type);
//		}

//		RowStore rowstore = new RowStore(orderSchema, "input/orders_small.csv", "\\|");
//		rowstore.load();
//		int tupleIndex = 0;
//		DBTuple tuple = rowstore.getRow(tupleIndex++);
//		for(int i = 0; i < tuple.fields.length; i++) {
//			System.out.println(tuple.fields[i]);
//		}
		RowStore rowstoreData = new RowStore(schema, "input/data.csv", ",");
        rowstoreData.load();
        
        RowStore rowstoreOrder = new RowStore(orderSchema, "input/orders_small.csv", "\\|");
        rowstoreOrder.load();
        
	    ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrder);
	    ch.epfl.dias.ops.volcano.Scan scanData = new ch.epfl.dias.ops.volcano.Scan(rowstoreData);
//	    scan.open();
//	    DBTuple tuple = scan.next();
//	    for(int i = 0; i < tuple.fields.length; i++) {
//			System.out.println(tuple.fields[i]);
//		}
//	    tuple = scan.next();
//	    for(int i = 0; i < tuple.fields.length; i++) {
//			System.out.println(tuple.fields[i]);
//		}
//	    scan.close();
//	    ch.epfl.dias.ops.volcano.Select select = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.EQ, 0, 32);
//	    select.open();
//	    tuple = select.next();
//	    if(tuple.eof) {
//	    	System.out.println("this is the end of tableTuple");
//	    }else {
//	    	for(int i = 0; i < tuple.fields.length; i++) {
//				System.out.println(tuple.fields[i]);
//			}
//	    }
//	    tuple = select.next();
//	    if(tuple.eof) {
//	    	System.out.println("this is the end of tableTuple");
//	    }else {
//	    	for(int i = 0; i < tuple.fields.length; i++) {
//				System.out.println(tuple.fields[i]);
//			}
//	    }
//	    select.close();
	  
	    
//	    int[] fieldNo = {0, 1, 8};
//	    ch.epfl.dias.ops.volcano.Project project = new ch.epfl.dias.ops.volcano.Project(scan, fieldNo);
//	    project.open();
//	    DBTuple tuple = project.next();
//	    while(!tuple.eof) {
//	    	for(int i = 0; i < tuple.fields.length; i++) {
//				System.out.println(tuple.fields[i]);
//			}
//	    	tuple = project.next();
//	    }
//		project.close();
	   
	    
//	    ch.epfl.dias.ops.volcano.HashJoin hashJoin = new ch.epfl.dias.ops.volcano.HashJoin(scanOrder, scanData, 0, 0);
//		hashJoin.open();
//	    DBTuple tuple = hashJoin.next();
//	    while(!tuple.eof) {
////	    	for(int i = 0; i < tuple.fields.length; i++) {
////				System.out.println(tuple.fields[i]);
////			}
//	    	tuple = hashJoin.next();
//	    }
//		hashJoin.close();
//		
//		hashJoin.open();
//		tuple = hashJoin.next();
//	    while(!tuple.eof) {
//	    	for(int i = 0; i < tuple.fields.length; i++) {
//				System.out.println(tuple.fields[i]);
//			}
//	    	tuple = hashJoin.next();
//	    }
//		hashJoin.close();
	    
	    ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(scanOrder, Aggregate.AVG, DataType.INT, 0);
	    agg.open();
	    DBTuple tuple = agg.next();
	    if(!tuple.eof) {
	    	for(int i = 0; i < tuple.fields.length; i++) {
				System.out.println(tuple.fields[i]);
			}
	    }
		agg.close();
//	    ColumnStore columnstoreData = new ColumnStore(schema, "input/data.csv", ",");
//		columnstoreData.load();
//		
//		ColumnStore columnstoreOrder = new ColumnStore(orderSchema, "input/orders_small.csv", "\\|", true);
//		columnstoreOrder.load();
//		int[] columnsToGet = {0, 2, 3};
//		DBColumn[] columnSet = columnstoreOrder.getColumns(columnsToGet);
//		for(int i = 0; i < columnSet.length; i++) {
//			Object[] column = columnSet[i].fields;
//			DataType type = columnSet[i].type;
//			System.out.println(type);
//			for(int j = 0; j < column.length; j++) {
//				System.out.println(column[j]);
//			}
//		}
 		

		// PAXStore paxstore = new PAXStore(orderSchema, "input/orders_small.csv", "\\|", 3);
		// paxstore.load();
		 
		// ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstore);
		// DBTuple currentTuple = scan.next();
		// while (!currentTuple.eof) {
		// 	System.out.println(currentTuple.getFieldAsInt(1));
		// 	currentTuple = scan.next();
		// }

		// ColumnStore columnstoreData = new ColumnStore(schema, "input/data.csv", ",");
		// columnstoreData.load();
		//
		// ch.epfl.dias.ops.block.Scan scan = new ch.epfl.dias.ops.block.Scan(columnstoreData);
		// ch.epfl.dias.ops.block.Select sel = new ch.epfl.dias.ops.block.Select(scan, BinaryOp.EQ, 3, 6);
		// ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 2);
		// DBColumn[] result = agg.execute();
		// int output = result[0].getAsInteger()[0];
		// System.out.println(output);
	}
}
