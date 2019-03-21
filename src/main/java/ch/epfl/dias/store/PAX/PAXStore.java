package ch.epfl.dias.store.PAX;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

public class PAXStore extends Store {

	// TODO: Add required structures
	public DataType[] schema;
	public String filename;
	public String delimiter;
	public int tuplesPerPage;
	public List<DBPAXpage> page = new ArrayList<>();

	public PAXStore(DataType[] schema, String filename, String delimiter, int tuplesPerPage) {
		// TODO: Implement
		this.schema = schema;
		this.filename = filename;
		this.delimiter = delimiter;
		this.tuplesPerPage = tuplesPerPage;
	}

	@Override
	public void load() throws IOException {
		// TODO: Implement
		Path pathToFile = Paths.get(filename);
		try (BufferedReader br = Files.newBufferedReader(pathToFile)){ 
			
			Object[][] primaryData = new Object[schema.length][tuplesPerPage];
			
			String line = br.readLine(); 
			int tupleIndex = 0;
			while (line != null) { 
				
				String[] attributes = line.split(delimiter);
				for(int i = 0; i < schema.length; i++) {
					primaryData[i][tupleIndex] = changeType(attributes[i], schema[i]);					
				}
				tupleIndex ++;
				if(tupleIndex == tuplesPerPage) {
					DBColumn[] PAXpage = convertToDBColumn(primaryData);
					page.add(new DBPAXpage(PAXpage, schema));
					primaryData = new Object[schema.length][tuplesPerPage];
					tupleIndex = 0;
				}
				line = br.readLine(); 	
			}
			
			if(tupleIndex != 0) {
				DBColumn[] PAXpage = convertToDBColumn(primaryData);
				page.add(new DBPAXpage(PAXpage, schema));
			}
		}catch (IOException ioe) {
			System.err.println(ioe);
		}
	}

	@Override
	public DBTuple getRow(int rownumber) {
		// TODO: Implement
		int pageNumber =  rownumber / tuplesPerPage;
		int pageRowNum = rownumber % tuplesPerPage;
		if(pageNumber >= page.size()) {
			return new DBTuple();
		}else {
			DBTuple tuple = page.get(pageNumber).getTuple(pageRowNum);
			return tuple;
		}
	}
	
	private Object changeType(String attribute, DataType type) {
		
		Object field = null;
		//Change from string to different dataType according to parameter type
		switch(type) {
		case INT:
			field = Integer.parseInt(attribute);
			break;
		case DOUBLE:
			field = Double.parseDouble(attribute);
			break;
		case BOOLEAN:
			field = Boolean.parseBoolean(attribute);
			break;
		case STRING:
			field = attribute;
			break;
		default:
			field = attribute;
			System.out.println("Something wrong with the datatype!");			
		}
		return field;
	}
	
	private DBColumn[] convertToDBColumn(Object[][] primaryData) {
		DBColumn[] PAXpage = new DBColumn[primaryData.length];
		int index = 0;
		for(Object[] miniPage : primaryData) {
			PAXpage[index] = new DBColumn(miniPage, schema[index]);
			index ++;
		}
		return PAXpage;
	}
}
