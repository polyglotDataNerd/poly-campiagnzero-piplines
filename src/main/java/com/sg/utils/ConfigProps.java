package com.sg.utils;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

public class ConfigProps implements Serializable {

    String pValue = "";


    public String getPropValues(String propName) throws IOException {
        String propfileName = "config.properties";
        try (InputStream ins = getClass().getClassLoader().getResourceAsStream(propfileName))
        //try (InputStream ins = new FileInputStream(propfileName))
        {
            Properties prop = new Properties();

            if (ins != null) {
                prop.load(ins);
            } else {
                throw new FileNotFoundException("property file " + propfileName + " not found");
            }

            pValue = prop.getProperty(propName);

        } catch (FileNotFoundException e) {
            System.out.println(e.getMessage());
        }

        return pValue;
    }


    public void setPropValues(String key, String value) {
        String propfileName = "config.properties";
        try {
            Parameters params = new Parameters();
            FileBasedConfigurationBuilder<FileBasedConfiguration> builder =
                    new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class)
                            .configure(params.properties()
                                    .setFileName(propfileName)
                            );
            Configuration config = builder.getConfiguration();
            config.setProperty(key, value);
            builder.save();


        } catch (Exception e) {
            System.out.println(e.getMessage());

        }

    }
}
