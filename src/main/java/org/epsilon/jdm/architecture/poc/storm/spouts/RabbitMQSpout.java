package org.epsilon.jdm.architecture.poc.storm.spouts;

import static org.epsilon.jdm.architecture.poc.storm.main.TopologyConstants.FIELD_LOGLINE;
import static org.epsilon.jdm.architecture.poc.storm.main.TopologyConstants.MAIN_STREAM;

import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang.math.RandomUtils;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class RabbitMQSpout extends BaseRichSpout {

	private static final long serialVersionUID = 8055599630988407030L;
	protected SpoutOutputCollector collector;
	protected static final String[] SAMPLE_MESSAGES = {
		"Login : el usuario 12345 ha accedido a la aplicación", 
		"Login : el usuario 67890 ha accedido a la aplicación", 
		"Logout : el usuario 67890 ha salido de la aplicación", 
		"Logout : el usuario 12345 ha salido de la aplicación", 
		"Read : a post",
		"Read : a post",
		"Read : a post",
		"Read : a post",
		"Read : a post",
		"Read : a post",
		"Read : a post",
		"Read : a post",
		"Read : a post",
		"Read : a post",
		"Write : a post",
		"Write : a comment",
		"Write : a comment",
		"Write : a comment",
		"Write : a comment"
	};

	@Override
	public void nextTuple() {
		Values tuple = new Values(
				SAMPLE_MESSAGES[
				                RandomUtils.nextInt(SAMPLE_MESSAGES.length)
				                ]
			);
		collector.emit(
				MAIN_STREAM
				, tuple
				, UUID.randomUUID().toString()
			);
	}

	@Override
	public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(MAIN_STREAM, new Fields(FIELD_LOGLINE));
	}
}