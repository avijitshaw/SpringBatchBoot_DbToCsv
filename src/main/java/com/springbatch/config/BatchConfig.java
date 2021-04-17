package com.springbatch.config;

import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.RowMapper;

import com.springbatch.batch.PlayerItemProcessor;
import com.springbatch.model.Player;

@Configuration
public class BatchConfig {

	@Autowired
	JobBuilderFactory jobBuilderFactory;

	@Autowired
	StepBuilderFactory stepBuilderFactory;

	@Autowired
	DataSource dataSource;

	@Bean
	public JdbcCursorItemReader<Player> itemReader() {
		JdbcCursorItemReader<Player> ir = new JdbcCursorItemReader<Player>();
		ir.setDataSource(dataSource);
		ir.setSql("select name,age,subject from player_entity");
		ir.setRowMapper(new RowMapper<Player>() {

			@Override
			public Player mapRow(ResultSet rs, int rowNum) throws SQLException {
				Player p = new Player();
				p.setName(rs.getString(1));
				p.setAge(rs.getString(2));
				p.setSubject(rs.getString(3));
				return p;
			}
		});
		return ir;
	}

	@Bean
	public PlayerItemProcessor itemProcessor() {
		return new PlayerItemProcessor();
	}

	@Bean
	public FlatFileItemWriter<Player> itemWriter() {
		FlatFileItemWriter<Player> ir = new FlatFileItemWriter<Player>();
		ir.setResource(new ClassPathResource("players.csv"));

		DelimitedLineAggregator<Player> lineAggr = new DelimitedLineAggregator<Player>();
		lineAggr.setDelimiter(",");

		BeanWrapperFieldExtractor<Player> beanWrapperFieldExtractor = new BeanWrapperFieldExtractor<Player>();
		beanWrapperFieldExtractor.setNames(new String[] { "name", "age", "subject" });
		lineAggr.setFieldExtractor(beanWrapperFieldExtractor);
		ir.setLineAggregator(lineAggr);
		return ir;
	}
	
	@Bean
	public Step step1() {
		return stepBuilderFactory.get("stepName-1")
					.<Player,Player>chunk(100)
					.reader(itemReader())
					.processor(itemProcessor())
					.writer(itemWriter())
					.build();
	}
	
	@Bean
	Job job1() {
		return jobBuilderFactory.get("jobName-1")
					.incrementer(new RunIdIncrementer())
					.flow(step1())
					.end()
					.build();
	}
}
