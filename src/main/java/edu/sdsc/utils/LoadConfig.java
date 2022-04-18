package edu.sdsc.utils;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStream;

public class LoadConfig {
    public static JsonObject getConfig(String polystoreName)  {
        JsonObject configObject = Json.createObjectBuilder().build();
        try {
            InputStream fis = new FileInputStream("config.json");
            JsonReader reader = Json.createReader(fis);

            configObject = reader.readObject();

            reader.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return configObject.getJsonObject(polystoreName);
    }

}
