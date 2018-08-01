package gov.va.aes.vear.dataloader;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

import gov.va.aes.vear.dataloader.main.VearDataLoader;

@SpringBootApplication
public class Application {

    @Autowired
    VearDataLoader dataloader;

    public static void main(String[] args) {
	SpringApplication.run(Application.class, args);
    }

    @Bean
    public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
	return args -> {
	    dataloader.process();

	    ((ConfigurableApplicationContext) ctx).close();
	};
    }
}