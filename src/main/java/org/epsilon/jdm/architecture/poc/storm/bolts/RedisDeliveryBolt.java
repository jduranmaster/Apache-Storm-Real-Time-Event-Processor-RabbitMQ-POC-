package org.epsilon.jdm.architecture.poc.storm.bolts;

import static org.epsilon.jdm.architecture.poc.storm.main.TopologyConstants.FIELD_DOCUMENT;
import static org.epsilon.jdm.architecture.poc.storm.main.TopologyConstants.MAIN_STREAM;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class RedisDeliveryBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -3928732369636317586L;

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		final String document = tuple.getString(0);
		
		System.out.println("*** Publicando en Redis = " + document);
		
		collector.emit(MAIN_STREAM, new Values(document));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(MAIN_STREAM, new Fields(FIELD_DOCUMENT));
	}

}
