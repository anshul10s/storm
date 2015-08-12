package storm.kafka;

import java.util.List;

import org.joda.time.Period;

/**
 * These Spouts are dependent on progress of other consumer of the same topic.
 * 
 * There are use-cases where multiple consumer are consuming off same topic,
 * and child consumer cannot go ahead of parent consumers. 
 * 
 * One of the approach is to create another topic and write into it when parent Consumer has read, but these will unnecessary duplicate the data
 * 
 * 
 * 
 * @author anshul.gupta
 *
 */
public class DependentConsumerSpoutConfig extends SpoutConfig {

	String dependConsumerZkRoot;
	String depConsumerId; ///Module name of depConsumer
	int updateFrequency = Period.seconds(2).getMillis();
	
	public DependentConsumerSpoutConfig(BrokerHosts hosts, String topic,
			String zkRoot, String id, String depConsumerId) {
		super(hosts, topic, zkRoot, id);
		this.depConsumerId = depConsumerId;
	}

}
