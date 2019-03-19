package es.upm.fi.dia.oeg.utils;

import es.upm.fi.dia.oeg.model.RDB;
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphCSVProperties;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphCSVRunnerFactory;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBProperties;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBRunnerFactory;
import es.upm.fi.dia.oeg.rmlc.processor.MorphRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

public class RunQuery {


    static Logger log = LoggerFactory.getLogger(MorphRunner.class.getPackage().toString());
    //run morph
    public static void runBatchMorph(RDB rdb){
        MorphRDBProperties properties = getMorphProperties(rdb);

        Path path = Paths.get("examples/"+rdb.getName()+".r2rml.ttl");
        properties.setOutputFilePath(path.getParent().getParent().toAbsolutePath()+""+path.getFileName().toString().replace(".r2rml.ttl","")+"-results.nt");

        try {
            MorphCSVRunnerFactory runnerFactory = new MorphCSVRunnerFactory();
            MorphBaseRunner runner = runnerFactory.createRunner(properties);
            runner.run();
            log.info("Materialization made correctly");
        } catch(Exception e) {
            e.printStackTrace();
            log.info("Error occured: " + e.getMessage());
        }

    }

    public static void runQueryMorph(RDB rdb, String query){
        MorphRDBProperties properties = getMorphProperties(rdb);

        Path path = Paths.get(query);
        properties.setOutputFilePath(path.toAbsolutePath().toString().replace(".rq","")+"-results.xml");
        properties.setQueryFilePath(query);

        try {
            MorphRDBRunnerFactory runnerFactory = new MorphRDBRunnerFactory();
            MorphBaseRunner runner = runnerFactory.createRunner(properties);
            runner.run();
            log.info("Evaluation query correctly");
        } catch(Exception e) {
            e.printStackTrace();
            log.info("Error occured: " + e.getMessage());
        }
    }

    private static MorphRDBProperties getMorphProperties(RDB rdb){
        MorphRDBProperties properties = new MorphRDBProperties();

        properties.setMappingDocumentFilePath("examples/"+rdb.getName()+".r2rml.ttl");

        properties.setDatabaseDriver("org.h2.Driver");
        properties.setDatabaseType("h2");
        properties.setDatabaseURL("jdbc:h2:mem:"+rdb);
        properties.setNoOfDatabase(1);
        properties.setDatabaseName(rdb.getName());
        properties.setDatabaseUser("sa");
        properties.setDatabasePassword("");
        properties.setDatabaseURL("");

        return properties;
    }

    //run ontop


}
