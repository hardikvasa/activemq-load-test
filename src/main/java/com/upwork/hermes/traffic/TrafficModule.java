package com.upwork.hermes.traffic;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.jms.pool.PooledConnectionFactory;

import java.util.concurrent.TimeUnit;

public class TrafficModule extends AbstractModule {

    private final String endpoint;

    private final String producerUser;
    private final String producerPassword;

    private final String consumerUser;
    private final String consumerPassword;

    public TrafficModule(String endpoint, String producerUser, String producerPassword, String consumerUser, String consumerPassword) {
        this.endpoint = endpoint;
        this.producerUser = producerUser;
        this.producerPassword = producerPassword;
        this.consumerUser = consumerUser;
        this.consumerPassword = consumerPassword;
    }

    @Override
    protected void configure() {
        bind(MetricRegistry.class).asEagerSingleton();
    }

    @Provides
    @Singleton
    private ConsoleReporter consoleReporter(final MetricRegistry metricRegistry) {
        return ConsoleReporter
                .forRegistry(metricRegistry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
    }

    @Provides
    @Singleton
    @Named("producer-connections")
    private ActiveMQConnectionFactory producerActiveMQConnectionFactory() {
        final ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory(endpoint);
        activeMQConnectionFactory.setUserName(producerUser);
        activeMQConnectionFactory.setPassword(producerPassword);
        activeMQConnectionFactory.setClientIDPrefix("Producer");

        // Many high performance applications are designed to be tolerate a small amount of message loss in failure scenarios.
        // If your application has been designed in this fashion, you can enable the use of async sends to increase throughput even when using persistent messages.
        // (http://activemq.apache.org/async-sends.html)
        activeMQConnectionFactory.setAlwaysSyncSend(true);
        activeMQConnectionFactory.setDispatchAsync(false);
        activeMQConnectionFactory.setSendTimeout(10000);
        activeMQConnectionFactory.setConnectResponseTimeout(10000);
        activeMQConnectionFactory.setMessagePrioritySupported(true);

        return activeMQConnectionFactory;
    }

    @Provides
    @Singleton
    @Named("consumer-connections")
    private ActiveMQConnectionFactory consumerActiveMQConnectionFactory() {

        final ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(endpoint);
        connectionFactory.setUserName(consumerUser);
        connectionFactory.setPassword(consumerPassword);
        connectionFactory.setClientIDPrefix("CONSUMER_TEST");

        // Configuration
        connectionFactory.setAlwaysSyncSend(true);
        connectionFactory.setAlwaysSessionAsync(true);
        connectionFactory.setOptimizeAcknowledge(false);
        connectionFactory.setDispatchAsync(false);
        connectionFactory.setSendTimeout(5000);
        connectionFactory.setConnectResponseTimeout(5000);
        connectionFactory.setMessagePrioritySupported(true);

        // PrefetchPolicy
        {
            ActiveMQPrefetchPolicy prefetchPolicy = new ActiveMQPrefetchPolicy();
            prefetchPolicy.setAll(1);
            connectionFactory.setPrefetchPolicy(prefetchPolicy);
        }

        // Redelivery Policies
        {
            RedeliveryPolicy queuePolicy = new RedeliveryPolicy();
            queuePolicy.setInitialRedeliveryDelay(0);
            queuePolicy.setUseExponentialBackOff(false);
            queuePolicy.setMaximumRedeliveries(0);

            RedeliveryPolicy dlQueuePolicy = new RedeliveryPolicy();
            dlQueuePolicy.setMaximumRedeliveries(-1);
            dlQueuePolicy.setUseExponentialBackOff(true);
            dlQueuePolicy.setRedeliveryDelay(50);
            dlQueuePolicy.setInitialRedeliveryDelay(50);
            dlQueuePolicy.setMaximumRedeliveryDelay(300000);
            dlQueuePolicy.setBackOffMultiplier(2);

            connectionFactory.getRedeliveryPolicy().setMaximumRedeliveries(0);
            connectionFactory.getRedeliveryPolicyMap().put(new ActiveMQQueue("Consumer.>"), queuePolicy);
            connectionFactory.getRedeliveryPolicyMap().put(new ActiveMQQueue("DLQ.Consumer.>"), dlQueuePolicy);
        }

        return connectionFactory;
    }

    @Provides
    @Singleton
    private PooledConnectionFactory pooledConnectionFactory(@Named("producer-connections") final ActiveMQConnectionFactory activeMQConnectionFactory) {
        final PooledConnectionFactory pooledConnectionFactory = new PooledConnectionFactory();
        pooledConnectionFactory.setConnectionFactory(activeMQConnectionFactory);

        pooledConnectionFactory.setMaxConnections(32);
        return pooledConnectionFactory;
    }
}
