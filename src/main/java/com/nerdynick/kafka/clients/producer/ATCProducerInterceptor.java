package com.nerdynick.kafka.clients.producer;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.configuration2.CompositeConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nerdynick.commons.configuration.utils.TransformConfigUtils;

public class ATCProducerInterceptor<K, V> implements ProducerInterceptor<K, V> {
	private static final Logger LOG = LoggerFactory.getLogger(ATCProducerInterceptor.class);
	
	private AdminClient client;
	private Set<String> topicCache = new HashSet<>();
	
	private Optional<Integer> defaultNumPartitions = Optional.empty();
	private Optional<Short> defaultReplicationFactor = Optional.empty();
	
	@Override
	public void configure(Map<String, ?> configs) {
		LOG.debug("Recieved Configs: {}", configs);
		final MapConfiguration config = new MapConfiguration(configs);
		
		final Configuration atcConfigs = config.subset("atc");
		
		final CompositeConfiguration clientConfig = new CompositeConfiguration();
		clientConfig.append(atcConfigs.subset("adminclient"));
		clientConfig.append(config);
		
		client = KafkaAdminClient.create(TransformConfigUtils.toMap(clientConfig));
		
		int defaultNumPartitions = atcConfigs.getInt("num.partitions", 0);
		short defaultReplicationFactor = atcConfigs.getShort("default.replication.factor", (short) 0);
		if(defaultNumPartitions>0) {
			this.defaultNumPartitions = Optional.of(defaultNumPartitions);
		}
		if(defaultReplicationFactor>0) {
			this.defaultReplicationFactor = Optional.of(defaultReplicationFactor);
		}
		LOG.debug("Defaults: Parts={} Rep={}", this.defaultNumPartitions, this.defaultReplicationFactor);
		
		LOG.info("Looking up current topics");
		client.listTopics().names().whenComplete((ts, e)->{
			LOG.debug("Topic Lookup Results: {}", ts, e);
			if(e != null) {
				LOG.error("Failed to get topics", e);
				throw new RuntimeException("Failed to get topics", e);
			}
			if(ts != null) {
				topicCache.addAll(ts);
			}
		});
	}

	@Override
	public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
		if(!topicCache.contains(record.topic())) {
			final NewTopic nt = new NewTopic(record.topic(), this.defaultNumPartitions, this.defaultReplicationFactor);
			LOG.info("Creating Topic {}", nt);
			
			client.createTopics(Arrays.asList(nt)).all().whenComplete((v,e)->{
				if(e != null) {
					if(e instanceof TopicExistsException) {
						LOG.info("Topic Already Exists", e);
					} else {
						throw new RuntimeException("Failed to create topic", e);
					}
				}
				topicCache.add(nt.name());
			});
		}
		return record;
	}

	@Override
	public void onAcknowledgement(RecordMetadata metadata, Exception exception) {}

	@Override
	public void close() {
		topicCache.clear();
		if(client != null) {
			client.close();
		}
	}

}
