package es.upm.fi.dia.oeg.rdb;


import es.upm.fi.dia.oeg.Utils;
import es.upm.fi.dia.oeg.rmlc.api.model.*;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;


public class RDBProcessor{

    private HashMap<String,Collection<TriplesMap>> mapping;
    private RDBConexion rdbConexion;
    private JSONArray datasets;
    private String rdb="";
    private static final Logger _log = LoggerFactory.getLogger(RDBProcessor.class);


    public RDBProcessor(Utils utils){
        this.datasets = utils.getConfig().getJSONArray("datasets");
        this.rdbConexion = new RDBConexion();
        this.mapping = utils.getMappings();
    }

    public void run(){
        DatasetImport.downloadAndUnzip(this.datasets);
    }

    public void createDatabases(){
        checkDatatypes();
        createRDB();
    }

    public void checkDatatypes(){
        for(Map.Entry<String, Collection<TriplesMap>> entry : mapping.entrySet()) {
            this.checkTypes(entry.getValue(),entry.getKey());
        }
    }


    public void createRDB(){
        for(Map.Entry<String, Collection<TriplesMap>> entry : mapping.entrySet()) {
            String name = entry.getKey();
            HashMap<String,HashMap<String,String>> functions = new HashMap<>();
            HashMap<String,HashMap<String,String>> joinFunctions = new HashMap<>();
            CSVTransformation csvTransformation = new CSVTransformation(null, null, name,false);
            HashMap<String,String[]> firstRow = csvTransformation.getFirstRow();
            entry.getValue().forEach(triplesMap -> {
                for(Map.Entry<String,String[]> firstRowEntry : firstRow.entrySet()){
                    String tableName =((SQLBaseTableOrView) triplesMap.getLogicalSource()).getTableName().toLowerCase().replace(".csv","");
                    if(firstRowEntry.getKey().equals(tableName)){
                        rdb+=this.createTable(triplesMap, firstRowEntry.getValue(), tableName.toUpperCase());
                        functions.put(tableName,getColumnsFromFunctions(triplesMap.getPredicateObjectMaps()));
                        joinFunctions.putAll(getJoinFunctions(triplesMap.getPredicateObjectMaps(),tableName));
                        break;
                    }
                }

            });

            try{
                BufferedWriter writer = new BufferedWriter
                        (new OutputStreamWriter(new FileOutputStream("datasets/"+name+"/database.sql"), StandardCharsets.UTF_8));
                writer.write(rdb);
                writer.close();
                rdbConexion.createDatabase(name,rdb);
                rdbConexion.updateDataWithFunctions(functions,name);
                rdbConexion.updateDataWithFunctions(joinFunctions,name);
                rdb="";
            }catch (IOException e){
                _log.error("Error writing the SQL file: "+e.getMessage());
            }
        }

    }


    private void checkTypes(Collection<TriplesMap> triplesMaps, String rdb){
        for (TriplesMap triplesMap : triplesMaps) {
            for(PredicateObjectMap predicateObjectMap: triplesMap.getPredicateObjectMaps()){
                for(ObjectMap objectMap: predicateObjectMap.getObjectMaps()) {
                    if (objectMap.getDatatype() != null) {
                        StringTokenizer st = new StringTokenizer(objectMap.getDatatype().getIRIString(),"#");
                        if(st.nextToken().equals("http://www.w3.org/2001/XMLSchema")) {
                            //checking numbers
                            CSVTransformation csvTransformation = new CSVTransformation(objectMap.getColumn(),((SQLBaseTableOrView) triplesMap.getLogicalSource()).getTableName(), rdb, true);
                            String type = st.nextToken();
                            if (type.matches("decimal|integer|double")) {
                                //ToDo accept SQL views.
                                csvTransformation.transformNumbers();
                            }
                            //checking dates
                            else if (type.equals("date")) {
                                csvTransformation.transformDates();
                            }
                            //ToDo checking dateTimes
                            else if (type.equals("dateTime")) {
                                csvTransformation.transformDateTimes();
                            } else if (type.equals("time")) { //does it make sense?
                                csvTransformation.transformTime();
                            } else if (type.equals("boolean")) {
                                csvTransformation.transformBoolean();
                            }
                        }
                    }
                }
            }
        }

    }


    private String createTable(TriplesMap tripleMap, String[] firstCSVRow, String tableName){
        ArrayList<String> primaryKeys = getPrimaryKeys(tripleMap.getSubjectMap());

        String table="DROP TABLE IF EXISTS "+tableName+";\nCREATE TABLE "+tableName+" ";
        table+="(";

        for(String field : firstCSVRow){
            table += "`"+ field.toUpperCase().trim()+"` "+getTypeFromColumn(tripleMap.getPredicateObjectMaps(),field)+",";
        }

        table=table.substring(0,table.length()-1);
        if(!primaryKeys.isEmpty()) {
            table += ",PRIMARY KEY (";
            for (String p : primaryKeys) {
                table += p + ",";
            }
            table=table.substring(0,table.length()-1);
            table += ")";
        }

        table+=");\n";
        return table;
    }

    private ArrayList<String> getPrimaryKeys(SubjectMap s){
        ArrayList<String> primaryKeys = new ArrayList<>();

        if(s.getColumn()!=null){
            primaryKeys.add(s.getColumn());
        }
        else if(s.getTemplateString()!=null){
            for(String t : s.getTemplate().getColumnNames()){
                primaryKeys.add(t);
            }
        }
        return primaryKeys;

    }


    private String getTypeFromColumn(List<PredicateObjectMap> predicates, String columnName){
        String type=null;
        for(PredicateObjectMap p: predicates){
            for(ObjectMap o: p.getObjectMaps()){
                if(this.checkObject(o,columnName)) {
                    type = getTypeFromObject(o);
                    break;
                }
            }
        }
        if(type==null){
            type="VARCHAR(200)";
        }
        return type;
    }

    private boolean checkObject(ObjectMap o, String columnName){
        List<String> columns = this.getColumnNames(o);
        if(columns.contains(columnName.toUpperCase())){
            return true;
        }
        else
            return false;
    }

    private String getTypeFromObject(ObjectMap object){
        String type;

        if(object.getDatatype()!=null){
            String[] st = object.getDatatype().getIRIString().split("#");
            if(st[0].equals("http://www.w3.org/2001/XMLSchema")) {
                String semanticType = st[1];
                if (semanticType.matches("integer")) {
                    type = "INT";
                } else if (semanticType.matches("double|decimal")) {
                    type = "DOUBLE";
                }
                //checking dates
                else if (semanticType.matches("date")) {
                    type = "DATE";
                } else if (semanticType.matches("boolean")) {
                    type = "BOOL";
                } else {
                    type = "VARCHAR(200)";
                }
            }
            else {
                type = "VARCHAR(200)";
            }

        }
        else {
            type="VARCHAR(200)";
        }

        return type;
    }


    private List<String> getColumnNames(ObjectMap o){
        List<String> columNames = new ArrayList<>();
        if(o.getColumn()!=null){
            columNames.add(o.getColumn());
        }
        else if (o.getTemplate()!=null){
            columNames.addAll(o.getTemplate().getColumnNames());
        }
        return  columNames;
    }


    private HashMap<String,String> getColumnsFromFunctions(List<PredicateObjectMap> predicateObjectMaps){
        HashMap<String,String> columnsFunctions = new HashMap<>();

        for(PredicateObjectMap p : predicateObjectMaps){
            List<ObjectMap> objectMaps = p.getObjectMaps();
            for(ObjectMap o : objectMaps){
                if(o.getFunction()!=null && !o.getFunction().isEmpty()){
                    List<PredicateMap> predicateMaps = p.getPredicateMaps();
                    for(PredicateMap pm : predicateMaps){
                       String t= pm.getConstant().ntriplesString();
                        columnsFunctions.put(t.split("#")[1].replace(">","") + " VARCHAR(200)",o.getFunction());
                    }
                }
            }
        }
        return columnsFunctions;

    }

    private HashMap<String,HashMap<String,String>> getJoinFunctions(List<PredicateObjectMap> predicateObjectMaps, String child_table_name){
        HashMap<String,HashMap<String,String>> joinFunction = new HashMap<>();
        for(PredicateObjectMap p : predicateObjectMaps){
            List<RefObjectMap> refObjectMaps = p.getRefObjectMaps();
            List<PredicateMap>  predicateMaps = p.getPredicateMaps();
            for(RefObjectMap refObjectMap : refObjectMaps){
                String parent_table_name = ((SQLBaseTableOrView) refObjectMap.getParentMap().getLogicalSource()).getTableName().toLowerCase().replace(".csv","");
                List<Join> join = refObjectMap.getJoinConditions();
                for(Join j : join ){
                    String child_function = j.getChild().getFunctions();
                    String parent_function = j.getParent().getFunctions();
                    for(PredicateMap pm : predicateMaps){
                        HashMap<String, String> functions_column = new HashMap<>();
                        String t= pm.getConstant().ntriplesString().split("#")[1].replace(">","") + " VARCHAR(200)";
                        if(!parent_function.isEmpty()) {
                            functions_column.put(t, parent_function);
                            joinFunction.put(parent_table_name, (HashMap<String, String>) functions_column.clone());
                        }
                        functions_column.clear();
                        if(!child_function.isEmpty()) {
                            functions_column.put(t, child_function);
                            joinFunction.put(child_table_name, (HashMap<String, String>) functions_column.clone());
                        }

                    }
                }
            }
        }
        return joinFunction;

    }




}
