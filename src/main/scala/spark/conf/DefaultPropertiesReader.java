package spark.conf;

import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DefaultPropertiesReader {

    final protected Map<String, String> properties;

    public DefaultPropertiesReader(String filePath) throws Exception {
        properties = new HashMap<>();
        Properties temp = loadProperties(filePath);
        for (final String name: temp.stringPropertyNames())
            properties.put(name, temp.getProperty(name));
    }

    private Properties loadProperties(String filePath) throws Exception {
        Properties properties = new Properties();
        try (FileReader reader = new FileReader(filePath)) {
            properties.load(reader);
        }
        return properties;
    }

    public Map<String, String> getProperties(){ return properties;}

    public String getByKey(String key){
        return properties.get(key);
    }
}
