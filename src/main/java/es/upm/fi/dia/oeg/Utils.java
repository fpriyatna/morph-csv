package es.upm.fi.dia.oeg;

import es.upm.fi.dia.oeg.rmlc.api.binding.jena.JenaRMLCMappingManager;
import es.upm.fi.dia.oeg.rmlc.api.model.TriplesMap;
import es.upm.fi.dia.oeg.rmlc.api.model.impl.InvalidRMLCMappingException;
import org.apache.commons.io.IOUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.stream.Collectors;

public class Utils {

    private static final Logger _log = LoggerFactory.getLogger(Utils.class);
    private  HashMap<String,Collection<TriplesMap>> mappings;
    private JSONObject config;


    public Utils (String path){
       this.mappings = new HashMap<>();
       setConfig(path);
       downloadMappings();
    }



    public void downloadMappings(){
        JSONArray datasets = config.getJSONArray("datasets");
        for(Object d : datasets) {
            String url = ((JSONObject) d).getString("mapping");
            String name =  ((JSONObject) d).getString("databaseName");
            String content="";
            try {
                if (url.matches("http.*")) {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(new URL(url).openStream()));
                    content = reader.lines().collect(Collectors.joining());
                } else {
                    try {
                        content = new String(Files.readAllBytes(Paths.get(url)));
                    } catch (IOException e) {
                        _log.error("Error reading the mapping file: " + e.getMessage());
                    }
                }
                File f = new File("datasets/" + name + "/mapping.rmlc.ttl");
                BufferedWriter writer = new BufferedWriter(new FileWriter(f, false));
                writer.write(content);
                writer.close();
            } catch (Exception e) {
                _log.error("Error getting the mapping content: " + e.getMessage());
            }
        }
    }

    public String getMappingContent(String path){
        String content="";
        try {
            content = new String(Files.readAllBytes(Paths.get(path)));
        }catch (IOException e){
            _log.error("Error reading the content of mapping file: "+e.getMessage());
        }
        return content;
    }

    public String getMappingPath(String databaseName){
        return "datasets/"+databaseName+"/mapping.rmlc.ttl";
    }

    public HashMap<String, Collection<TriplesMap>> getMappings() {
        return mappings;
    }

    public Collection<TriplesMap> getMapping(String key){
        return mappings.get(key);
    }

    public void setMapping(HashMap<String, Collection<TriplesMap>> mapping) {
        this.mappings = mapping;
    }

    public void loadMappings(){
        JSONArray datasets = config.getJSONArray("datasets");
        for(Object object: datasets) {
            String database_name = ((JSONObject) object).getString("databaseName");
            JenaRMLCMappingManager mm = JenaRMLCMappingManager.getInstance();
            Model m = ModelFactory.createDefaultModel();
            m = m.read(this.getMappingPath(database_name));
            try {
                Collection<TriplesMap> triplesMaps = mm.importMappings(m);
                mappings.put(database_name,triplesMaps);
            } catch (InvalidRMLCMappingException e) {
                _log.error("Exception in read mapping: " + e.getMessage());
            }
        }
    }

    public JSONObject getConfig() {
        return config;
    }

    public void setConfig(String path){
        try {
            this.config = new JSONObject(IOUtils.toString(new FileReader(path)));
        } catch (IOException e) {
            _log.error("Error processing the configuration json file: "+e.getMessage());
        }
    }

}
