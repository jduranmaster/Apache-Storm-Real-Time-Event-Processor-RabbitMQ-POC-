package org.epsilon.jdm.architecture.poc.storm.bolts;

import static org.epsilon.jdm.architecture.poc.storm.main.TopologyConstants.FIELD_CRMCAMPAIGN;
import static org.epsilon.jdm.architecture.poc.storm.main.TopologyConstants.MAIN_STREAM;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CrmFunctionBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -3928732369636317586L;

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		final String customerId = tuple.getString(0);
		final String crmCampaign = getCrmCampaign(customerId);
		collector.emit(MAIN_STREAM, new Values(crmCampaign));
	}

	private String getCrmCampaign(String customerId) {
		// goes to a MongoDB for CRM campaign
		if ("12345".equals(customerId)) {
			return "{ \"id\" : 123450001, \"image\" : \"publi1.png\" }";
		} else {
			return "{ \"id\" : 678900001, \"image\" : \"publi2.png\" }";
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(MAIN_STREAM, new Fields(FIELD_CRMCAMPAIGN));
	}

}
