package org.epsilon.jdm.architecture.poc.storm.spouts;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.epsilon.jdm.architecture.poc.storm.main.TopologyConstants;
import org.epsilon.jdm.architecture.poc.storm.spouts.RabbitMQSpout;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

public class RabbitMQSpoutTest {

	@Test
	public void testOpen() {
		RabbitMQSpout spout = new RabbitMQSpout();
		SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
		spout.open(null, null, collector);
		assertEquals("Original collector was stored internally", collector, spout.collector);
	}

	@Test
	public void testDeclareOutputFields() {
		RabbitMQSpout spout = new RabbitMQSpout();
		OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				String streamId = (String) invocation.getArguments()[0];
				Fields fields = (Fields) invocation.getArguments()[1];
				if (!TopologyConstants.MAIN_STREAM.equals(streamId)
						|| !TopologyConstants.FIELD_LOGLINE.equals(fields.get(0))) {
					throw new IllegalArgumentException("Stream and fields has not been setted.");
				}
				return null;
			}
		}).when(declarer).declareStream(anyString(), any(Fields.class));
		spout.declareOutputFields(declarer);

	}

	@SuppressWarnings("unchecked")
	@Test
	public void testNextTuple() {
		RabbitMQSpout spout = new RabbitMQSpout();
		SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				String streamId = (String) invocation.getArguments()[0];
				List tuple = (List) invocation.getArguments()[1];
				if (!TopologyConstants.MAIN_STREAM.equals(streamId)
						|| tuple.isEmpty()
						|| StringUtils.isEmpty((String) tuple.get(0)) ) {
					throw new IllegalArgumentException("Stream and fields has not been setted.");
				}
				return null;
			}
		}).when(collector).emit(anyString(), any(List.class));
		spout.open(null, null, collector);
		spout.nextTuple();
	}

}
