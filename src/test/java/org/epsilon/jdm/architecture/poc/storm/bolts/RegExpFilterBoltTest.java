package org.epsilon.jdm.architecture.poc.storm.bolts;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.epsilon.jdm.architecture.poc.storm.bolts.RegExpFilterBolt;
import org.epsilon.jdm.architecture.poc.storm.main.TopologyConstants;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class RegExpFilterBoltTest {

	@Test
	public void testExecuteWithValidWord() {
		testExecute("Login : el usuario 12345 ha accedido a la aplicación", 1); 
		testExecute("Login : el usuario 67890 ha accedido a la aplicación", 1); 
	}
	
	@Test
	public void testExecuteWithInvalidWord() {
		testExecute("Logout : el usuario 67890 ha salido de la aplicación", 0); 
		testExecute("Logout : el usuario 12345 ha salido de la aplicación", 0); 
		testExecute("Read : a post", 0);
		testExecute("Read : a post", 0);
	}
	
	private void testExecute(final String acceptedWord, int count) {
		RegExpFilterBolt bolt = new RegExpFilterBolt("Login : el usuario (\\d{5}) ha accedido a la aplicación");
		Tuple tuple = mock(Tuple.class);
		when(tuple.getString(0)).thenReturn(acceptedWord);
		BasicOutputCollector collector = mock(BasicOutputCollector.class);
		final List<String> words = new ArrayList<String>();
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
				words.add(word);
				return null;
			}
		}).when(collector).emit(anyString(), any(List.class));
		bolt.execute(tuple, collector);
		Assert.assertEquals("Expected word count", count, words.size());
	}

	@Test
	public void testDeclareOutputFields() {
		RegExpFilterBolt bolt = new RegExpFilterBolt("");
		OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				String streamId = (String) invocation.getArguments()[0];
				Fields fields = (Fields) invocation.getArguments()[1];
				if (!TopologyConstants.MAIN_STREAM.equals(streamId)
						|| !TopologyConstants.FIELD_CUSTOMERID.equals(fields.get(0))) {
					throw new IllegalArgumentException("Stream and fields has not been setted.");
				}
				return null;
			}
		}).when(declarer).declareStream(anyString(), any(Fields.class));
		bolt.declareOutputFields(declarer);
	}

}
