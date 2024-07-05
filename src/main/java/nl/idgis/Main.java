package nl.idgis;

import nl.idgis.importer.GioImporter;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@ComponentScan
@PropertySource("classpath:application.properties")
@PropertySource(value = "file:${spring.config.location}", ignoreResourceNotFound = true)
public class Main {

    public static void main(String[] args) {
        try {
            ApplicationContext context = new AnnotationConfigApplicationContext(Main.class);

            GioImporter gioImporter = context.getBean(GioImporter.class);
            gioImporter.importGio();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
