package org.epsilon.jdm.architecture.poc.storm.bolts;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.epsilon.jdm.architecture.poc.storm.bolts.RedisDeliveryBolt;
import org.epsilon.jdm.architecture.poc.storm.main.TopologyConstants;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class RedisDeliveryBoltTest {

	@Test
	public void testExecute() {
		RedisDeliveryBolt bolt = new RedisDeliveryBolt();
		Tuple tuple = mock(Tuple.class);
		when(tuple.getString(0)).thenReturn("{ documento }");
		BasicOutputCollector collector = mock(BasicOutputCollector.class);
		final List<String> documentos = new ArrayList<String>();
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				String streamId = (String) invocation.getArguments()[0];
				List tuple = (List) invocation.getArguments()[1];
				String word = (String) tuple.get(0);
				if (!TopologyConstants.MAIN_STREAM.equals(streamId)
						|| StringUtils.isEmpty(word) ) {
					throw new IllegalArgumentException("Stream and fields has not been setted.");
				}
				if (!word.equals("{ documento }")) {
					throw new IllegalArgumentException("Emmiting an unexpected document.");
				}
				documentos.add(word);
				return null;
			}
		}).when(collector).emit(anyString(), any(List.class));
		bolt.execute(tuple, collector);
		Assert.assertEquals("Expected document count", 1, documentos.size());
		Assert.assertEquals("Expected document content", "{ documento }", documentos.get(0));
	}

	@Test
	public void testDeclareOutputFields() {
		RedisDeliveryBolt bolt = new RedisDeliveryBolt();
		OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				String streamId = (String) invocation.getArguments()[0];
				Fields fields = (Fields) invocation.getArguments()[1];
				if (!TopologyConstants.MAIN_STREAM.equals(streamId)
						|| !TopologyConstants.FIELD_DOCUMENT.equals(fields.get(0))) {
					throw new IllegalArgumentException("Stream and fields has not been setted.");
				}
				return null;
			}
		}).when(declarer).declareStream(anyString(), any(Fields.class));
		bolt.declareOutputFields(declarer);
	}

}
