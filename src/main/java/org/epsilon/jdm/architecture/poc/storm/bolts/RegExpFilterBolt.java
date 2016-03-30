package org.epsilon.jdm.architecture.poc.storm.bolts;

import static org.epsilon.jdm.architecture.poc.storm.main.TopologyConstants.FIELD_CUSTOMERID;
import static org.epsilon.jdm.architecture.poc.storm.main.TopologyConstants.MAIN_STREAM;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class RegExpFilterBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -3928732369636317586L;
	final Pattern pattern;
	public RegExpFilterBolt(final String regEx) {
		pattern = Pattern.compile(regEx, Pattern.CASE_INSENSITIVE);
	}
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		final String text = tuple.getString(0);
		if (StringUtils.isEmpty(text)) {
			return;
		}
		String firstGroupValue = getFirstGroupValue(text);
		if (StringUtils.isNotEmpty(firstGroupValue)) {
			collector.emit(MAIN_STREAM, new Values(firstGroupValue));
		}
	}

	private String getFirstGroupValue(String text) {
		try {
			final Matcher matcher = pattern.matcher(text);
			if (matcher.matches()) {
				return matcher.group(1);
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
		return null;
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(MAIN_STREAM, new Fields(FIELD_CUSTOMERID));
	}

}
