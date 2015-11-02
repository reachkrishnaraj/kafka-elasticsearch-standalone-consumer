package org.elasticsearch.kafka.indexer.jmx;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.kafka.indexer.jobs.IndexerJobManager;
import org.elasticsearch.kafka.indexer.jobs.IndexerJobStatus;
import org.elasticsearch.kafka.indexer.jobs.IndexerJobStatusEnum;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class KafkaEsIndexerStatusTest {

	List<IndexerJobStatus> statuses = new ArrayList<IndexerJobStatus>();
	
	KafkaEsIndexerStatus kafkaEsIndexerStatus;
	
	@Mock
	private IndexerJobManager jobManager;
	
	@Before
	public void init(){
		statuses.add(new IndexerJobStatus(123, IndexerJobStatusEnum.Failed, 1));
		statuses.add(new IndexerJobStatus(124, IndexerJobStatusEnum.Cancelled, 2));
		statuses.add(new IndexerJobStatus(125, IndexerJobStatusEnum.Stopped, 3));
		statuses.add(new IndexerJobStatus(126, IndexerJobStatusEnum.Started, 4));
		statuses.add(new IndexerJobStatus(127, IndexerJobStatusEnum.Failed, 5));

		when(jobManager.getJobStatuses()).thenReturn(statuses);
		kafkaEsIndexerStatus = new KafkaEsIndexerStatus(jobManager);
	}
	
	@Test
	public void getStatuses(){
		assertEquals(kafkaEsIndexerStatus.getStatuses().equals(statuses), true);
	}
	
	@Test
	public void getCountOfFailedJobs(){
		assertEquals(kafkaEsIndexerStatus.getCountOfFailedJobs(), 2);
	}
	
	@Test
	public void getCountOfCancelledJobs(){
		assertEquals(kafkaEsIndexerStatus.getCountOfCancelledJobs(), 1);
	}
	
	@Test
	public void getCountOfStoppedJobs(){
		assertEquals(kafkaEsIndexerStatus.getCountOfStoppedJobs(), 1);
	}

}
