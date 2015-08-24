package storm.kafka;

import java.io.Serializable;
import java.util.Map;

import jline.internal.Log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.KafkaSpout.MessageAndRealOffset;
import backtype.storm.metric.api.CountMetric;

public class DepdendentConsumetPartitionManager extends PartitionManager {

	public static final Logger LOG = LoggerFactory.getLogger(DepdendentConsumetPartitionManager.class);
	
	private DependentConsumerSpoutConfig _dependentConsumerSpoutConfig;
	private CachedOffset dependentOffsetCached;
	private final CountMetric _fetchBlockedByDepedentCounter;

	public DepdendentConsumetPartitionManager(
			DynamicPartitionConnections connections, String topologyInstanceId,
			ZkState state, Map stormConf, DependentConsumerSpoutConfig dependentConsumerSpoutConfig, Partition id) {
		super(connections, topologyInstanceId, state, stormConf, dependentConsumerSpoutConfig, id);
		this._dependentConsumerSpoutConfig = dependentConsumerSpoutConfig;
		dependentOffsetCached = fetchOffset();
        LOG.info("Dependent offset -{} for topologyInstanceId - {}", dependentOffsetCached, topologyInstanceId);
        
        _fetchBlockedByDepedentCounter =  new CountMetric();
	}


	private CachedOffset fetchOffset() {
		dependentOffsetCached = null;
        String path = dependetConsumerCommitPath();
        try {
            Map<Object, Object> json = _state.readJSON(path);
            LOG.info("Dependent Read partition information from: " + path +  "  --> " + json );
            if (json != null) {
               return new CachedOffset((Long) json.get("offset"));
            }
            Log.error("Dependenct Consumer - {}, offset json null from ZK. Partition Id - {}", _dependentConsumerSpoutConfig.depConsumerId, _partition.getId());
            return new CachedOffset(0l);
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
		boolean result = false;
		long currentOffset = toEmit.offset;
		if(currentOffset < this.getAndRefreshDependentOffset())
			result = true;
		
		if(!result) {
			_fetchBlockedByDepedentCounter.incr();
		}
		
		//Adding logs only when blocked or debug explicitly enabled
		if(!result || LOG.isDebugEnabled())
			LOG.info("isBehindDependentConsumer: Current Offset - {}, Dependent Parent Offest - {}. Parition Id - {}. Result - {}" , toEmit.offset , dependentOffsetCached.getOffset(), _partition.getId(), result );
		return result;
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
