package eu.cloudtm;

import java.io.*;
import java.util.Properties;

/**
 * @author Pedro Ruivo
 * @since 1.0
 */
public class Utils {

    private static final ClassLoader[] CLASS_LOADERS = {
            Utils.class.getClassLoader(),
            ClassLoader.getSystemClassLoader()
    };

    public static void safeClose(Closeable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (IOException e) {
            //ignored
        }
    }

    public static Properties loadProperties(String filePath) {
        Properties properties = new Properties();
        InputStream inputStream = tryOpenFile(filePath);
        if (inputStream == null) {
            inputStream = openResource(filePath);
        }
        if (inputStream != null) {
            try {
                properties.load(inputStream);
            } catch (IOException e) {
                //ignored
            } finally {
                safeClose(inputStream);
            }
        }
        return properties;
    }

    public static InputStream tryOpenFile(String filePath) {
        try {
            return new FileInputStream(filePath);
        } catch (FileNotFoundException e) {
            return null;
        }
    }

    public static InputStream openResource(String resource) {
        InputStream stream = tryOpenResource(Thread.currentThread().getContextClassLoader(), resource);
        if (stream != null) {
            return stream;
        }
        for (ClassLoader classLoader : CLASS_LOADERS) {
            stream = tryOpenResource(classLoader, resource);
            if (stream != null) {
                return stream;
            }
        }
        return null;
    }

    public static InputStream tryOpenResource(ClassLoader loader, String resource) {
        if (loader == null || resource == null || resource.isEmpty()) {
            return null;
        }
        return loader.getResourceAsStream(resource);
    }

}
