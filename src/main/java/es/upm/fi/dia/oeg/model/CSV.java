package es.upm.fi.dia.oeg.model;


import com.opencsv.CSVReader;

import java.io.*;
import java.net.URL;
import java.nio.file.Path;
import java.util.List;

public class CSV {

    private String nameFile;
    private String url;
    private String parentUrl;
    private CSVReader reader;
    private List<String[]> rows;

    public CSV(String nameFile,String url, CSVReader reader) {
        this.nameFile = nameFile;
        this.url = url;
        this.reader = reader;
    }

    public CSV(String nameFile, String url, Path csv) {
        this.nameFile = nameFile;
        this.url = url;
        setRows(csv);
    }

    public CSV(String nameFile, List<String[]> rows){
        this.nameFile =nameFile;
        this.rows = rows;
    }

    public CSV(String nameFile,String url, String parentUrl, List<String[]> rows){
        this.nameFile =nameFile;
        this.url = url;
        this.parentUrl = parentUrl;
        this.rows = rows;
    }


    public CSV(String nameFile, String url, URL csv) {
        this.nameFile = nameFile;
        this.url = url;
        setRows(csv);
    }

    public String getNameFile() {
        return nameFile;
    }

    public void setNameFile(String nameFile) {
        this.nameFile = nameFile;
    }

    public List<String[]> getRows() {
        return rows;
    }

    public String getParentUrl() {
        return parentUrl;
    }

    public void setParentUrl(String parentUrl) {
        this.parentUrl = parentUrl;
    }

    public void setRows (List<String[]> rows){
        this.rows = rows;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setRows (Path csv){

        try {
            this.reader = new CSVReader(new FileReader(csv.toAbsolutePath().toString()));
            this.rows = reader.readAll();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void setRows (URL url){
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()));
            this.reader = new CSVReader(reader);
            this.rows = this.reader.readAll();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
