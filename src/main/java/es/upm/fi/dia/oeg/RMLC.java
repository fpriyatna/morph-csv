package es.upm.fi.dia.oeg;

import es.upm.fi.dia.oeg.iterator.IteratorTransformer;
import es.upm.fi.dia.oeg.rdb.RDBProcessor;
import es.upm.fi.dia.oeg.translation.MappingTransformation;
import org.apache.commons.cli.CommandLine;


/**
 * RMLC: RDF Mapping Language for heterogeneous CSV files
 * @author : David Chaves
 */
public class RMLC
{
    public static void main( String[] args )
    {

        CommandLine commandLine = CommandLineProcessor.parseArguments(args);

        if(commandLine.getOptions().length < 2 || commandLine.getOptions().length > 3 ){
            CommandLineProcessor.displayHelp();
        }

        String config = commandLine.getOptionValue("c");
        String query = commandLine.getOptionValue("q");

        Utils utils = new Utils(config);

        RDBProcessor rdbProcessor = new RDBProcessor(utils);
        rdbProcessor.run();

        IteratorTransformer iteratorTransformer = new IteratorTransformer(utils);
        iteratorTransformer.run();

        utils.loadMappings();

        rdbProcessor.createDatabases();

        MappingTransformation mappingTransformation = new MappingTransformation();
        mappingTransformation.generateR2RML(utils);



    }
}
