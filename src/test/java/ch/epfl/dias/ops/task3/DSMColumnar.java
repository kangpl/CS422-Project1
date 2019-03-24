package ch.epfl.dias.ops.task3;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.ops.volcano.Project;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

public class DSMColumnar {
	DataType[] orderSchema;
    DataType[] lineitemSchema;
    
    ColumnStore columnstoreOrder;
    ColumnStore columnstoreLineItem;
    
    @Before
    public void init() throws IOException  {
    	
        orderSchema = new DataType[]{
                DataType.INT,
                DataType.INT,
                DataType.STRING,
                DataType.DOUBLE,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.INT,
                DataType.STRING};

        lineitemSchema = new DataType[]{
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.DOUBLE,
                DataType.DOUBLE,
                DataType.DOUBLE,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING};
        
        columnstoreOrder = new ColumnStore(orderSchema, "input/orders_big.csv", "\\|", true);
        columnstoreOrder.load();
        
        columnstoreLineItem = new ColumnStore(lineitemSchema, "input/lineitem_big.csv", "\\|", true);
        columnstoreLineItem.load();        
    }
    
	@Test
	public void query1(){
		/* SELECT L_ORDERKEY L_SHIPDATE FROM lineitem L WHERE L_ORDERKEY > 666666*/    
	    ch.epfl.dias.ops.columnar.Scan scan = new ch.epfl.dias.ops.columnar.Scan(columnstoreLineItem);
	    ch.epfl.dias.ops.columnar.Select sel = new ch.epfl.dias.ops.columnar.Select(scan, BinaryOp.GT, 0, 666666);
	    ch.epfl.dias.ops.columnar.Project proj = new ch.epfl.dias.ops.columnar.Project(sel, new int[] {0, 10});
		
		// This query should return only one result
		DBColumn[] result = proj.execute();
		int outputField0 = result[0].getAsInteger()[result[0].getAsInteger().length - 1];
        String outputField1 = result[1].getAsString()[result[1].getAsString().length -1];

        System.out.println(outputField0);
        System.out.println(outputField1);
        assertTrue(outputField0 == 999939);
	}
	
	@Test
    public void query2(){
        /* SELECT L_PARTKEY, O_CUSTKEY FROM lineitem L, order O WHERE (L_ORDERKEY=O_ORDERKEY)*/
		
        ch.epfl.dias.ops.columnar.Scan scanLineitem = new ch.epfl.dias.ops.columnar.Scan(columnstoreLineItem);
        ch.epfl.dias.ops.columnar.Scan scanOrder = new ch.epfl.dias.ops.columnar.Scan(columnstoreOrder);
    
        // Projection
        ch.epfl.dias.ops.columnar.Project projLineItem = new ch.epfl.dias.ops.columnar.Project(scanLineitem, new int[]{0,1});
        ch.epfl.dias.ops.columnar.Project projOrder = new ch.epfl.dias.ops.columnar.Project(scanOrder, new int[]{0,1});

        ch.epfl.dias.ops.columnar.Join join = new ch.epfl.dias.ops.columnar.Join(projLineItem ,projOrder, 0, 0);
    
        ch.epfl.dias.ops.columnar.Project projFinal = new ch.epfl.dias.ops.columnar.Project(join, new int[]{1,3});

        DBColumn[] result = projFinal.execute();
        int outputLineitem = result[0].getAsInteger()[result[0].getAsInteger().length - 1];
        int outputOrder = result[1].getAsInteger()[result[1].getAsInteger().length -1];
        System.out.println(outputLineitem);
        System.out.println(outputOrder);
        assertTrue(outputLineitem == 118515);
        assertTrue(outputOrder == 41275);
    }
	
	@Test
    public void query3(){
        /* SELECT AVG(L_DISCOUNT) FROM lineitem */	    
        ch.epfl.dias.ops.columnar.Scan scan = new ch.epfl.dias.ops.columnar.Scan(columnstoreLineItem);
        ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(scan, Aggregate.AVG, DataType.DOUBLE, 6);
        
        // This query should return only one result
        DBColumn[] result = agg.execute();
        double output = result[0].getAsDouble()[0];
        System.out.println(output);
        output = 0.05006691999986567;
        assertTrue(output == 0.05006691999986567);
    }
}
