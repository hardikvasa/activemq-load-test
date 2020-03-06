package com.upwork.hermes.traffic;

import com.codahale.metrics.Timer;
import org.apache.activemq.ActiveMQSession;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class Subscriber {
    private final String queueName;
    private final Set<String> routingKeys;
    private final int numberOfConsumers;
    private final int delay;

    private final ConnectionFactory connectionFactory;
    private final Timer timer;
    private List<Runnable> shutdownHooks = new ArrayList<>();

    public Subscriber(final String queueName, final Set<String> routingKeys, final int numberOfConsumers, final int delay, final ConnectionFactory connectionFactory, Timer timer) {
        this.queueName = queueName;
        this.routingKeys = routingKeys;
        this.numberOfConsumers = numberOfConsumers;
        this.delay = delay;

        this.connectionFactory = requireNonNull(connectionFactory);
        this.timer = requireNonNull(timer);
    }

    public String getQueueName() {
        return queueName;
    }

    public void run() {
        try {
            final Connection consumerConnection = connectionFactory.createConnection();
            consumerConnection.setClientID(String.format("%s.Subscriber", queueName));
            consumerConnection.start();

            Map<Session, MessageConsumer> sessionConsumerMap = new HashMap<>();
            for (int i = 0; i < numberOfConsumers; i++) {
                final Session consumerSession = consumerConnection.createSession(false, ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE);
                final Queue destination = consumerSession.createQueue(this.queueName);
                final MessageConsumer consumer = consumerSession.createConsumer(destination);
                consumer.setMessageListener((message) -> {
                    process(consumerSession, consumer, message);
                });
                sessionConsumerMap.put(consumerSession, consumer);
            }

            shutdownHooks.add(() -> closeConsumers(sessionConsumerMap, consumerConnection));
        } catch (JMSException ex) {
            ex.printStackTrace();
        }
    }

    private void process(final Session consumerSession, final MessageConsumer consumer, final Message message) {
        try {

            // 1. Calculate round trip latency and update timer
            final long timestamp = message.getLongProperty("timestamp");
            final long latency = System.currentTimeMillis() - timestamp;
            timer.update(latency, TimeUnit.MILLISECONDS);

            // 2. Delay acknowledge to emulate latencies
            try {
                Thread.sleep(delay);
            } catch (InterruptedException ignored) {
                ignored.printStackTrace();
            }

            // 3. Acknowledge
            message.acknowledge();
        } catch (Exception e) {
            // ERROR => send to DLQ
            System.err.println("Failed to process message:" + e.getMessage());
            e.printStackTrace();
            try {
                consumerSession.recover();
            } catch (JMSException ex) {
                ex.printStackTrace();
            }
        }
    }

    private void closeConsumers(Map<Session, MessageConsumer> sessionConsumerMap, Connection consumerConnection) {
        System.out.println("Closing consumers ");
        try {
            for (Map.Entry<Session, MessageConsumer> entry : sessionConsumerMap.entrySet()) {
                entry.getValue().setMessageListener(null);
                entry.getValue().close();
                entry.getKey().close();
            }

            consumerConnection.close();
        } catch (JMSException ignored) {
            ignored.printStackTrace();
        }
    }

    public void shutdown() {
        shutdownHooks.forEach(Runnable::run); // <-- Runnable::run is mapped to the closeConsumer function when adding the hooks
        shutdownHooks.clear();
    }
}