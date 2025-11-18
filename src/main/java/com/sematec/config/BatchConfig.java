package com.sematec.config;

import javax.sql.DataSource;

import jdk.jfr.Registered;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.VirtualThreadTaskExecutor;
import org.springframework.jdbc.datasource.init.DataSourceInitializer;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.transaction.PlatformTransactionManager;

import com.sematec.listener.JobCompletionListener;
import com.sematec.step.Processor;
import com.sematec.step.Reader;
import com.sematec.step.Writer;

@Configuration
public class BatchConfig {

	private final JobRepository jobRepository;

	private final PlatformTransactionManager transactionManager;

	private final  DataSource dataSource;

    public BatchConfig(JobRepository jobRepository, PlatformTransactionManager transactionManager, DataSource dataSource) {
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
        this.dataSource = dataSource;
    }

    @Bean
	public DataSourceInitializer dataSourceInitializer() {
		DataSourceInitializer initializer = new DataSourceInitializer();
		initializer.setDataSource(dataSource);
		ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
		populator.addScript(new ClassPathResource("org/springframework/batch/core/schema-h2.sql"));
		initializer.setDatabasePopulator(populator);
		return initializer;
	}

	@Bean
	public Job processJob() {
		return new JobBuilder("processJob", jobRepository)
				.incrementer(new RunIdIncrementer())
				.listener(listener())
				.start(flow1())
				.split(virtualThreadTaskExecutor())
				.add(flow2())
				.next(step4())
				.end()
				.build();
	}

	@Bean
	public VirtualThreadTaskExecutor virtualThreadTaskExecutor() {
		return new VirtualThreadTaskExecutor();
	}

	@Bean
	public Flow flow1() {
		return new FlowBuilder<Flow>("flow1")
				.start(step1())
				.next(step2())
				.build();
	}

	@Bean
	public Flow flow2() {
		return new FlowBuilder<Flow>("flow2")
				.start(step3())
				.build();
	}

	@Bean
	public Step step1() {
		return new StepBuilder("step1", jobRepository)
				.<String, String> chunk(1, transactionManager)
				.reader(new Reader())
				.processor(new Processor())
				.writer(new Writer())
				.build();
	}

	@Bean
	public Step step2() {
		return new StepBuilder("step2", jobRepository)
				.<String, String> chunk(1, transactionManager)
				.reader(new Reader())
				.processor(new Processor())
				.writer(new Writer())
				.build();
	}

	@Bean
	public Step step3() {
		return new StepBuilder("step3", jobRepository)
				.<String, String> chunk(1, transactionManager)
				.reader(new Reader())
				.processor(new Processor())
				.writer(new Writer())
				.build();
	}
	@Bean
	public Step step4() {
		return new StepBuilder("step4", jobRepository)
				.<String, String> chunk(1, transactionManager)
				.reader(new Reader())
				.processor(new Processor())
				.writer(new Writer())
				.build();
	}

	@Bean
	public JobExecutionListener listener() {
		return new JobCompletionListener();
	}

}
