package org.epsilon.jdm.architecture.poc.storm.main;

import static org.epsilon.jdm.architecture.poc.storm.main.TopologyConstants.MAIN_STREAM;

import org.apache.commons.lang.math.RandomUtils;
import org.epsilon.jdm.architecture.poc.storm.bolts.CrmFunctionBolt;
import org.epsilon.jdm.architecture.poc.storm.bolts.RedisDeliveryBolt;
import org.epsilon.jdm.architecture.poc.storm.bolts.RegExpFilterBolt;
import org.epsilon.jdm.architecture.poc.storm.spouts.RabbitMQSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class Main {

	public static void main(String[] args) {
		
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("rabbitMQSpout", new RabbitMQSpout(), 3);
		
		builder.setBolt("regExpFilterBolt", new RegExpFilterBolt("Login : el usuario (\\d{5}) ha accedido a la aplicación"), 3)
		.shuffleGrouping("rabbitMQSpout", MAIN_STREAM);
	
		builder.setBolt("crmFunctionBolt", new CrmFunctionBolt(), 6)
			.shuffleGrouping("regExpFilterBolt", MAIN_STREAM);
		
		builder.setBolt("redisDeliveryBolt", new RedisDeliveryBolt(), 3)
			.shuffleGrouping("crmFunctionBolt", MAIN_STREAM);

		Config conf = new Config();
		conf.setDebug(false);

		LocalCluster cluster = new LocalCluster();
		final String topologyName = "adesis-poc-" + RandomUtils.nextLong();
		cluster.submitTopology(topologyName, conf, builder.createTopology());
		Utils.sleep(60 * 1000);
		cluster.killTopology(topologyName);
		cluster.shutdown();
		
		// submit to cluster
		// conf.setNumWorkers(3);
		// StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
		
	}

}
