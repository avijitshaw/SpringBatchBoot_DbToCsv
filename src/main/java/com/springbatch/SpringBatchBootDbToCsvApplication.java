package com.springbatch;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableBatchProcessing
public class SpringBatchBootDbToCsvApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringBatchBootDbToCsvApplication.class, args);
	}

}
