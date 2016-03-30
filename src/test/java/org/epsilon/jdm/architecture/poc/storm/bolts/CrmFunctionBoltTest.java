package org.epsilon.jdm.architecture.poc.storm.bolts;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.epsilon.jdm.architecture.poc.storm.bolts.CrmFunctionBolt;
import org.epsilon.jdm.architecture.poc.storm.main.TopologyConstants;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class CrmFunctionBoltTest {

	@Test
	public void testExecute() {
		CrmFunctionBolt bolt = new CrmFunctionBolt();
		Tuple tuple = mock(Tuple.class);
		when(tuple.getString(0)).thenReturn("12345");
		BasicOutputCollector collector = mock(BasicOutputCollector.class);
		final List<String> crmCampaigns = new ArrayList<String>();
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				String streamId = (String) invocation.getArguments()[0];
				List tuple = (List) invocation.getArguments()[1];
				String actualCrmCampaign = (String) tuple.get(0);
				if (!TopologyConstants.MAIN_STREAM.equals(streamId)
						|| StringUtils.isEmpty(actualCrmCampaign) ) {
					throw new IllegalArgumentException("Stream and fields has not been setted.");
				}
				crmCampaigns.add(actualCrmCampaign);
				return null;
			}
		}).when(collector).emit(anyString(), any(List.class));
		bolt.execute(tuple, collector);
		assertEquals("Invalid count expected", 1, crmCampaigns.size());
		assertEquals("Invalid content expected", "{ \"id\" : 123450001, \"image\" : \"publi1.png\" }", crmCampaigns.get(0));
		
	}

	@Test
	public void testDeclareOutputFields() {
		CrmFunctionBolt bolt = new CrmFunctionBolt();
		OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				String streamId = (String) invocation.getArguments()[0];
				Fields fields = (Fields) invocation.getArguments()[1];
				if (!TopologyConstants.MAIN_STREAM.equals(streamId)
						|| !TopologyConstants.FIELD_CRMCAMPAIGN.equals(fields.get(0))) {
					throw new IllegalArgumentException("Stream and fields has not been setted.");
				}
				return null;
			}
		}).when(declarer).declareStream(anyString(), any(Fields.class));
		bolt.declareOutputFields(declarer);
	}

}
