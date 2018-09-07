package es.upm.fi.dia.oeg.rdb;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class RDBConexion {

    private String url;
    private Properties sqlProperties;
    private static final Logger _log = LoggerFactory.getLogger(RDBConexion.class);

    public RDBConexion(JSONObject config){
        this.url = config.getString("mysqlURL");
        sqlProperties = new Properties();
        sqlProperties.setProperty("user", config.getString("mysqluser"));
        sqlProperties.setProperty("password", config.getString("mysqlpassword"));
        sqlProperties.setProperty("useSSL", "false");
        sqlProperties.setProperty("autoReconnect", "true");
    }

    public void createDatabase(String rdb, String tables){

        try {
            long startTime = System.currentTimeMillis();
            Connection c = DriverManager.getConnection(this.url,sqlProperties);
            Statement s=c.createStatement();
            s.execute("CREATE DATABASE IF NOT EXISTS "+rdb+";");
            s.close();
            c.close();
            createTables(tables,rdb);
            long stopTime = System.currentTimeMillis();
            long elapsedTime = stopTime - startTime;
            _log.info("The "+rdb+" has been indexed in MySQL successfully in: "+elapsedTime+"ms");
        }catch (Exception e ){
            _log.error("Error connecting with MySQL: "+e.getMessage());
        }
    }

    public void createTables(String tables, String rdb){
        try {
            Connection c = DriverManager.getConnection(this.url+rdb,sqlProperties);
            Statement s=c.createStatement();
            String[] st = tables.split("\n");
            for(String saux : st) {
                s.execute(saux);
                if(saux.matches("CREATE TABLE .*")){
                    String tableName = saux.split("CREATE TABLE ")[1].split("\\(")[0].trim();
                    insertData(tableName,rdb,s);
                }
            }
            s.close();c.close();
        }catch (SQLException e){
            _log.error("Error creating the tables in the rdb "+rdb+": "+e.getMessage());
        }
    }



    public void insertData(String tableName, String rdb, Statement s){
        File folder = new File("datasets/"+rdb);
        File[] listOfFiles = folder.listFiles();
        for(File f : listOfFiles){
            if(tableName.toLowerCase().equals(f.getName().split("\\.")[0].toLowerCase())){
                try {
                    s.execute("LOAD DATA LOCAL INFILE '"+f.getAbsolutePath()+"' " +
                            "INTO TABLE " + tableName +
                            " FIELDS TERMINATED BY ',' " +
                            "OPTIONALLY ENCLOSED BY '\"' " +
                            "LINES TERMINATED BY '\n' " +
                            "IGNORE 1 ROWS");
                }catch (SQLException e){
                    _log.error("Error loading the CSV data in "+tableName+" of the rdb "+rdb+": "+e.getMessage());
                }
                break;
            }

        }

    }
}
