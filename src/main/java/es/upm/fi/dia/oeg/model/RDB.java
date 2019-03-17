package es.upm.fi.dia.oeg.model;

import org.apache.commons.io.Charsets;

import java.nio.file.Files;
import java.nio.file.Path;

public class RDB {

    private String name;
    private String content;

    public RDB(String name, String content) {
        this.name = name;
        this.content = content;
    }

    public RDB(String name, Path path) {
        this.name = name;
        setContent(path);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public void setContent (Path path){
        try {
            this.content = Files.readAllLines(path, Charsets.toCharset("UTF-8")).toString();
        }catch (Exception e){
            //ToDo log
        }
    }


}
