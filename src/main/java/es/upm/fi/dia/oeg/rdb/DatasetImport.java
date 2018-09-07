package es.upm.fi.dia.oeg.rdb;

import es.upm.fi.dia.oeg.Utils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.net.URL;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class DatasetImport {


    private static final Logger _log = LoggerFactory.getLogger(DatasetImport.class);

    public static void downloadAndUnzip(JSONArray datasets){
        try {
            for (Object object : datasets) {
                JSONObject dataset = (JSONObject) object;
                String relativePath= "datasets/"+dataset.getString("databaseName");
                File f = new File(relativePath);
                f.mkdir();
                if (dataset.getString("url").matches("http.*")) {
                    f = new File(relativePath+"/dataset."+dataset.getString("compression"));
                    FileUtils.copyURLToFile(new URL(dataset.getString("url")), f);
                }
                else {
                    FileUtils.copyDirectory(new File(dataset.getString("url")), f);
                }

                if(dataset.getString("compression").equals("zip")){
                    unzip(relativePath,f.getName());
                    f.delete();
                }
                else if(!dataset.getString("compression").isEmpty()){
                    _log.error("The compression of "+dataset.getString("databaseName")+" CSV set should be ZIP");
                }
            }

        }catch (Exception e){
            _log.error("URL is not correct: "+e.getMessage());
        }
    }


    private static void unzip (String path, String name){
        try {
            byte[] buffer = new byte[1024];
            ZipInputStream zis = new ZipInputStream(new FileInputStream(path+"/"+name));
            ZipEntry zipEntry = zis.getNextEntry();
            while (zipEntry != null) {
                String fileName = zipEntry.getName();
                File newFile = new File(path+"/"+fileName);
                FileOutputStream fos = new FileOutputStream(newFile);
                int len;
                while ((len = zis.read(buffer)) > 0) {
                    fos.write(buffer, 0, len);
                }
                fos.close();
                zipEntry = zis.getNextEntry();
            }
            zis.closeEntry();
            zis.close();
        }catch (Exception e){
            _log.error("Error during unzip process: "+e.getMessage());
        }
    }
}
