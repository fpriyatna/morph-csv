package es.upm.fi.dia.oeg;

import es.upm.fi.dia.oeg.model.Dataset;
import es.upm.fi.dia.oeg.model.RDB;
import es.upm.fi.dia.oeg.rdb.RDBGenerator;
import es.upm.fi.dia.oeg.translation.RMLC2R2RML;
import es.upm.fi.dia.oeg.translation.yarrrml2RMLC;
import es.upm.fi.dia.oeg.utils.CommandLineProcessor;
import es.upm.fi.dia.oeg.utils.Utils;
import org.apache.commons.cli.CommandLine;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;


/**
 * RMLC: RDF Mapping Language for heterogeneous CSV files
 * @author : David Chaves
 */
public class SATET
{
    public static void main( String[] args )
    {

        CommandLine commandLine = CommandLineProcessor.parseArguments(args);

        if(commandLine.getOptions().length < 2 || commandLine.getOptions().length > 3 ){
            CommandLineProcessor.displayHelp();
        }

        String configPath = commandLine.getOptionValue("c");
        String query = commandLine.getOptionValue("q");
        String engine = "";
        if(commandLine.getOptions().length==3){
            engine = commandLine.getOptionValue("e");
        }

        JSONArray config = Utils.readConfiguration(configPath);
        ArrayList<Dataset> datasetArrayList = new ArrayList<>();
        for(Object aux : config){
            JSONObject c = (JSONObject) aux;
            Dataset dataset = new Dataset(c.get("csvw").toString(),c.get("yarrrml").toString());
            //set rmlc mapping from yarrrml after the translation and csv web

            //dataset.setRmlcMappingC(CSVW2RMLC.translateCSVW2RMLC(dataset.getCsvw()));
            dataset.setRmlcMappingY(yarrrml2RMLC.translateYarrrml2RMLC(dataset.getYarrrmlMapping()));

            //generate RDB
            RDBGenerator rdbGenerator = new RDBGenerator(dataset);
            rdbGenerator.generateSchemaRDB();
            rdbGenerator.generateRDB();
            //generate R2RML
            RMLC2R2RML rmlc2R2RML = new RMLC2R2RML();
            rmlc2R2RML.generateR2RML(dataset.getRmlcMappingY());
            dataset.setR2rmlMapping(rmlc2R2RML.getR2RML());
            //load RDB

            //execute query
        }

        //csvw2rmlc


        //yarrrml+rmlc 2 rmlc


        //rdb generation


        //r2rml generation


        //execution





    }
}
