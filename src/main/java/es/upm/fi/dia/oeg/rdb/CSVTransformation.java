package es.upm.fi.dia.oeg.rdb;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

public class CSVTransformation {

    private String columnName;
    private String fileName;
    private File file;
    private List<String[]> csvBody;
    private String rdb;
    private Integer columnNumber;
    private static final Logger _log = LoggerFactory.getLogger(CSVTransformation.class);
    public static final String UTF8_BOM = "\uFEFF";

    public CSVTransformation(String columnName, String fileName,String rdb, boolean transformation) {
        this.rdb = rdb;
        if(columnName!=null && fileName!=null) {
            this.columnName = columnName.toLowerCase();
            this.fileName = fileName.toLowerCase();
        }
        if(transformation==true) {
            csvBody = getFileContent();
            columnNumber = getCSVRow();
        }

    }


    private Integer getCSVRow(){
        Integer cl=null;
        if(!csvBody.isEmpty()) {
            for (int c = 0; c < csvBody.get(0).length; c++) {
                if (csvBody.get(0)[c].equals(columnName) && csvBody.size()>1) {
                    cl = c;
                    break;
                }
            }
        }
        return cl;
    }

    public void transformNumbers(){
        boolean flag=true;
        if(columnNumber!=null) {
            for (int i = 1; i < csvBody.size(); i++) {
                String row = updateNumbersInCSV(csvBody.get(i)[columnNumber]);
                if (row == null) {
                    flag = false;
                    break;
                } else {
                    csvBody.get(i)[columnNumber] = row;
                }
            }
            if (flag)
                writeCSV();
        }
        else{
            _log.info("The column "+columnName+" does not exist in the CSV file "+fileName +" rdb: "+rdb);
        }
    }


    public void transformDates(){
        if(columnNumber != null ) {
            String pattern = checkType(csvBody.get(1)[columnNumber]);
            boolean flag = true;

            if (pattern == null) {
                _log.error("The column "+columnName+" from "+fileName+","+rdb+" it is a date but we do not take into account its pattern");
            } else {
                for (int i = 1; i < csvBody.size(); i++) {
                    String row = updateDateInCSV(csvBody.get(i)[columnNumber], pattern);
                    if (row == null) {
                        flag = false;
                        break;
                    } else {
                        csvBody.get(i)[columnNumber] = row;
                    }
                }
            }
            if (flag)
                writeCSV();
        }
        else{
            _log.info("The column "+columnName+" does not exist in the CSV file "+fileName+" rdb: "+rdb);
        }
    }

    public void transformDateTimes(){

    }

    public void transformTime(){

    }

    public void transformBoolean(){
        if(columnNumber != null ) {
            String lower = csvBody.get(1)[columnNumber].toLowerCase();
            if (NumberUtils.isNumber(csvBody.get(1)[columnNumber]) || lower.equals(csvBody.get(1)[columnNumber])) {
                _log.info("The column " + columnName + " is already in correct format in the CSV file "+fileName +" rdb: "+rdb);
            } else {
                for (int i = 1; i < csvBody.size(); i++) {
                    csvBody.get(i)[columnNumber] = updateBooleanInCSV(csvBody.get(i)[columnNumber]);
                }
                writeCSV();
            }
        }
        else{
            _log.info("The column "+columnName+" does not exist in the CSV file "+fileName +" rdb: "+rdb);
        }
    }


    private List<String[]>  getFileContent(){
        File folder = new File("datasets/"+rdb);
        File[] listOfFiles = folder.listFiles();
        List<String[]> body = new ArrayList<>();
        try {
            for (int i = 0; i < listOfFiles.length; i++) {
                String[] st = listOfFiles[i].getName().toLowerCase().split("\\.");
                if (st[0].equals(fileName)) {
                    file=listOfFiles[i];
                    break;
                }
            }
            if(file!=null) {
                CSVReader reader = new CSVReader(new FileReader(file));
                body = reader.readAll();
            }
        }catch (Exception e){
            _log.error("Error getting the file: "+e.getMessage());
        }
        return body;

    }

    public HashMap<String,String[]> getFirstRow(){
        File folder = new File("datasets/"+rdb+"/data");
        File[] listOfFiles = folder.listFiles();
        HashMap<String,String[]> firstRow = new HashMap<>();
        try {
            for (int i = 0; i < listOfFiles.length; i++) {
                String[] st = listOfFiles[i].getName().toLowerCase().split("\\.");
                CSVReader reader = new CSVReader(new FileReader(listOfFiles[i]));
                String[] s = reader.readNext();
                s=removeUTF8BOM(s);
                firstRow.put(st[0].toLowerCase(),s);
            }
        }catch (Exception e){
            _log.error("Error getting the file: "+e.getMessage());
        }
        return firstRow;
    }

    private String updateBooleanInCSV(String row){
        return row.toLowerCase();
    }

    private String updateNumbersInCSV(String row){
        if(row.matches(".*,.*")) {
            row= row.replace(",", ".");
            row = row.replace("\"", "");
        }
        else if(row.matches(".*'.*")) {
            row = row.replace("'", ".");
        }
        else if(row.matches(".*\\..*")) {
            row =null;
        }
        return row;

    }

    private String updateDateInCSV(String row, String pattern){

        if (pattern.matches("/|\\.")) {
                row = row.replace(pattern, "-");
        }

        else if (pattern.matches("\\s")) {
            String year = row.substring(0, 4);
            String month = row.substring(4, 6);
            String day = row.substring(6, 8);
            row = year + "-" + month + "-" + day;

        }
        else if (pattern.matches("-")) {
             row = null;
        }
        else if (pattern.matches("inv_.*")) {
             StringTokenizer st = new StringTokenizer(pattern,"-");st.nextToken();
             String p = st.nextToken();
             if(p.matches("/|\\.|-")){
                     row = row.replace(p, "");
             }
             String day = row.substring(0, 2);
             String month = row.substring(2, 4);
             String year = row.substring(4, 8);
             row = year + "-" + month + "-" + day;
        }

        return row;

    }

    private String checkType(String firstRow){
        if(firstRow.matches(DatePatterns.backslash)) {
            return "/";
        }
        else if(firstRow.matches(DatePatterns.point)){
            return ".";
        }
        else if(firstRow.matches(DatePatterns.line)){
            return "-";
        }
        else if(firstRow.matches(DatePatterns.wihtout_space)){
            return " ";
        }
        else if(firstRow.matches(DatePatterns.inv_backslash)) {
            return "inv_/";
        }
        else if(firstRow.matches(DatePatterns.inv_point)){
            return "inv_.";
        }
        else if(firstRow.matches(DatePatterns.inv_line)){
            return "inv_-";
        }
        else if(firstRow.matches(DatePatterns.inv_wihtout_space)){
            return "inv_";
        }
        else {
            return null;
        }
    }

    private void writeCSV(){
        try {
            CSVWriter writer = new CSVWriter(Files.newBufferedWriter(file.toPath(),StandardCharsets.UTF_8));
            writer.writeAll(csvBody);
            writer.flush();
            writer.close();
        }catch (IOException e){
            _log.error("Error writing CSV file: "+e.getMessage());
        }
    }

    private static String[] removeUTF8BOM(String[] s) {
        for (int i=0; i<s.length;i++) {
            if (s[i].startsWith(UTF8_BOM)) {
                s[i] = s[i].substring(1);
            }
        }
        return s;
    }
}
