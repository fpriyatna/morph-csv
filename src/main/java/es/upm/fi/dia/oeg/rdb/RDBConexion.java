package es.upm.fi.dia.oeg.rdb;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.relation.RelationSupport;
import java.io.File;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class RDBConexion {

    private static final Logger _log = LoggerFactory.getLogger(RDBConexion.class);

    public RDBConexion(){
        try{
            Class.forName ("org.h2.Driver");
        }catch (Exception e){
            _log.error("The H2 driver has not found");
        }
    }

    public void createDatabase(String rdb, String tables){

        try {
            long startTime = System.currentTimeMillis();
            createTables(tables,rdb);
            long stopTime = System.currentTimeMillis();
            long elapsedTime = stopTime - startTime;
            _log.info("The "+rdb+" has been indexed in H2 successfully in: "+elapsedTime+"ms");
        }catch (Exception e ){
            _log.error("Error connecting with H2: "+e.getMessage());
        }
    }

    public void createTables(String tables, String rdb){
        try {
            Class.forName ("org.h2.Driver");
            Connection c = DriverManager.getConnection("jdbc:h2:~/"+rdb, "sa", "");
            Statement s=c.createStatement();
            String[] st = tables.split("\n");
            for(String saux : st) {
                if(saux.matches("CREATE TABLE .*")){
                    String tableName = saux.split("CREATE TABLE ")[1].split("\\(")[0].trim();
                    String csv=getCSVFile(tableName,rdb);
                    saux = saux.substring(0,saux.length()-1)+" AS SELECT * FROM CSVREAD('"+csv+"');";
                    s.execute(saux);
                }
                else {
                    s.execute(saux);
                }
            }
            s.close();c.close();
        }catch (Exception e){
            _log.error("Error creating the tables in the rdb "+rdb+": "+e.getMessage());
        }
    }



    public String getCSVFile(String tableName, String rdb){
        File folder = new File("datasets/"+rdb+"/data");
        File[] listOfFiles = folder.listFiles();
        for(File f : listOfFiles){
            if(tableName.toLowerCase().equals(f.getName().split("\\.")[0].toLowerCase())){
                return f.getAbsolutePath();
            }
        }
        return "";
    }

    public void updateDataWithFunctions (HashMap<String,HashMap<String,String>> functions, String rdb){
        long startTime = System.currentTimeMillis();
        try {
            Connection c = DriverManager.getConnection("jdbc:h2:~/"+rdb, "sa", "");
            Statement s = c.createStatement();
            for(Map.Entry<String,HashMap<String,String>> entry : functions.entrySet()){
                String table_name = entry.getKey();
                HashMap<String,String> column_function = entry.getValue();
                for(Map.Entry<String,String> function_entry : column_function.entrySet()){
                    String alter_column = function_entry.getKey();
                    String function_exp = function_entry.getValue();
                    s.execute("ALTER TABLE "+table_name+" ADD "+alter_column+";");
                    s.execute("UPDATE "+table_name+" SET "+alter_column.split(" ")[0]+"="+function_exp+";");

                }
            }
            s.close();c.close();

        }catch (Exception e){
            _log.error("Error in update the table: "+e.getMessage());
        }
        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        _log.info("The "+rdb+" has been updated in H2 successfully in: "+elapsedTime+"ms");
    }
}
