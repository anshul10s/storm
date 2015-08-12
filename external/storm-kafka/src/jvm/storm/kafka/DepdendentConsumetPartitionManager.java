package storm.kafka;

import java.io.Serializable;
import java.util.Map;

import backtype.storm.metric.api.CountMetric;
import jline.internal.Log;
import storm.kafka.KafkaSpout.MessageAndRealOffset;

public class DepdendentConsumetPartitionManager extends PartitionManager {

	private DependentConsumerSpoutConfig _dependentConsumerSpoutConfig;
	private CachedOffset dependentOffsetCached;
	private final CountMetric _fetchBlockedByDepedentCounter;

	public DepdendentConsumetPartitionManager(
			DynamicPartitionConnections connections, String topologyInstanceId,
			ZkState state, Map stormConf, DependentConsumerSpoutConfig dependentConsumerSpoutConfig, Partition id) {
		super(connections, topologyInstanceId, state, stormConf, dependentConsumerSpoutConfig, id);
		this._dependentConsumerSpoutConfig = dependentConsumerSpoutConfig;
		dependentOffsetCached = fetchOffset();
        Log.info("Depndent offset -{} for topologyInstanceId - {}", dependentOffsetCached, topologyInstanceId);
        
        _fetchBlockedByDepedentCounter =  new CountMetric();
	}


	private CachedOffset fetchOffset() {
		dependentOffsetCached = null;
        String path = dependetConsumerCommitPath();
        try {
            Map<Object, Object> json = _state.readJSON(path);
            LOG.info("Read partition information from: " + path +  "  --> " + json );
            if (json != null) {
               return new CachedOffset((Long) json.get("offset"));
            }
            throw new RuntimeException(String.format("Dependenct Consumer - %s, offset json null from ZK", _dependentConsumerSpoutConfig.depConsumerId));
        } catch (Throwable e) {
            LOG.error("Error reading and/or parsing at ZkNode: " + path, e);
            throw new RuntimeException(String.format("Dependenct Consumer - %s, offset cannot be found from ZK", _dependentConsumerSpoutConfig.depConsumerId), e);
        }
	}

	
	private String dependetConsumerCommitPath() {
        return _spoutConfig.zkRoot + "/" + _dependentConsumerSpoutConfig.depConsumerId + "/" + _partition.getId();
    }
	
	
	

	@Override
	public Map getMetricsDataMap() {
		Map metricsDataMap = super.getMetricsDataMap();
		metricsDataMap.put(_partition + "/fetchAPIBlockedByDependentCounter", _fetchBlockedByDepedentCounter.getValueAndReset());
		return metricsDataMap;
	}


	@Override
	protected boolean isBehindDependentConsumer(MessageAndRealOffset toEmit) {
		long currentOffset = toEmit.offset;
		if(currentOffset < dependentOffsetCached.getOffset())
			return true;
		
		_fetchBlockedByDepedentCounter.incr();
		return false;
	}

	
	public Long getAndRefreshDependentOffset() {
		if(dependentOffsetCached == null || dependentOffsetCached.needUpdate(_dependentConsumerSpoutConfig.updateFrequency)) {
			dependentOffsetCached = fetchOffset();
		}
		return dependentOffsetCached.getOffset();
	}
	
	public static class CachedOffset implements Serializable {
		Long offset;
		long lastUpdateTime;
		
		public CachedOffset(Long offset) {
			this.offset = offset;
			this.lastUpdateTime = System.currentTimeMillis();
		}

		public Long getOffset() {
			return offset;
		}

		public void setOffset(Long offset) {
			this.offset = offset;
			this.lastUpdateTime = System.currentTimeMillis();
		}
		
		public boolean needUpdate(long updateFrequencyMills) {
			return System.currentTimeMillis() - lastUpdateTime > updateFrequencyMills;
		}

		@Override
		public String toString() {
			return "CachedOffset [offset=" + offset + ", lastUpdateTime="
					+ lastUpdateTime + "]";
		}
		
	}
	
	
}
