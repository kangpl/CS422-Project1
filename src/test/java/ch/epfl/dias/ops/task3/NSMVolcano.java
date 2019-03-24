package ch.epfl.dias.ops.task3;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.ops.volcano.Project;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;
import ch.epfl.dias.store.row.RowStore;

public class NSMVolcano {
	DataType[] orderSchema;
    DataType[] lineitemSchema;
    
    RowStore rowstoreOrder;
    RowStore rowstoreLineItem;
    
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
        
        rowstoreOrder = new RowStore(orderSchema, "input/orders_big.csv", "\\|");
        rowstoreOrder.load();
        
        rowstoreLineItem = new RowStore(lineitemSchema, "input/lineitem_big.csv", "\\|");
        rowstoreLineItem.load();        
    }
    
	@Test
	public void query1(){
		/* SELECT L_ORDERKEY L_SHIPDATE FROM lineitem L WHERE L_ORDERKEY > 666666*/    
	    ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
	    ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.GT, 0, 666666);
	    ch.epfl.dias.ops.volcano.Project proj = new ch.epfl.dias.ops.volcano.Project(sel, new int[] {0, 10});
	
		proj.open();
		
		// This query should return only one result
		DBTuple result = proj.next();
        int outputField0 = 0;
        String outputField1 = null;
        while(!result.eof){
            outputField0 = result.getFieldAsInt(0);
            outputField1 = result.getFieldAsString(1);
            result = proj.next();
        }
        proj.close();
        System.out.println(outputField0);
        System.out.println(outputField1);
        assertTrue(outputField0 == 999939);
	}
	
	@Test
    public void query2(){
        /* SELECT L_PARTKEY, O_CUSTKEY FROM lineitem L, order O WHERE (L_ORDERKEY=O_ORDERKEY)*/
		
        ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
        ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrder);
    
        // Projection
        ch.epfl.dias.ops.volcano.Project projLineItem = new ch.epfl.dias.ops.volcano.Project(scanLineitem, new int[]{0,1});
        ch.epfl.dias.ops.volcano.Project projOrder = new ch.epfl.dias.ops.volcano.Project(scanOrder, new int[]{0,1});

        ch.epfl.dias.ops.volcano.HashJoin join = new ch.epfl.dias.ops.volcano.HashJoin(projLineItem ,projOrder, 0, 0);
    
        ch.epfl.dias.ops.volcano.Project projFinal = new ch.epfl.dias.ops.volcano.Project(join, new int[]{1,3});
        
        projFinal.open();

        DBTuple result = projFinal.next();
        int outputLineitem = 0;
        int outputOrder = 0;
        while(!result.eof){
            outputLineitem = result.getFieldAsInt(0);   
            outputOrder = result.getFieldAsInt(1);
            result = projFinal.next();
        }
        projFinal.close();
        System.out.println(outputLineitem);
        System.out.println(outputOrder);
        assertTrue(outputLineitem == 118515);
        assertTrue(outputOrder == 41275);
    }
	
	@Test
    public void query3(){
        /* SELECT AVG(L_DISCOUNT) FROM lineitem */	    
        ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
        ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(scan, Aggregate.AVG, DataType.DOUBLE, 6);
    
        agg.open();
        
        // This query should return only one result
        DBTuple result = agg.next();
        double output = result.getFieldAsDouble(0);
        agg.close();
        System.out.println(output);
        output = 0.05006691999986567;
        assertTrue(output == 0.05006691999986567);
    }
}
