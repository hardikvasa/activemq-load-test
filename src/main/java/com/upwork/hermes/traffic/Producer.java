package com.upwork.hermes.traffic;

import com.codahale.metrics.Timer;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

public class Producer {
    private final String uid;
    private final String routingKey;
    private final String topicName;
    private final int producerDelay;
    private final ConnectionFactory connectionFactory;
    private final Timer timer;

    // Configuration
    private ScheduledExecutorService scheduledExecutorServiceSender = Executors.newScheduledThreadPool(4);
    private ExecutorService executorServiceSender = Executors.newFixedThreadPool(PRODUCER_THREADS);
    private AtomicInteger currentActiveThreads = new AtomicInteger(0);
    private final AtomicInteger count = new AtomicInteger();
    private static final int PRODUCER_THREADS = 20;

    public Producer(final String routingKey, final String topicName, final int producerDelay, final ConnectionFactory connectionFactory, final Timer timer) {
        this.uid = "producer-" + System.currentTimeMillis();
        this.routingKey = routingKey;
        this.topicName = topicName;
        this.producerDelay = producerDelay;
        this.connectionFactory = requireNonNull(connectionFactory);
        this.timer = timer;
    }

    public String getTopicName() {
        return topicName;
    }

    public void run() {
        scheduledExecutorServiceSender.scheduleAtFixedRate(() -> sendMessageAsync(),
                new Random().nextInt(producerDelay) + producerDelay,
                producerDelay,
                TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        executorServiceSender.shutdown();
        scheduledExecutorServiceSender.shutdown();
    }


    public void sendMessageAsync() {
        if (currentActiveThreads.get() >= PRODUCER_THREADS) {
            return;
        }
        executorServiceSender.submit(() -> {
            try {
                produceMessage();
            } catch (JMSException e) {
                System.err.println("Error sending message: " + e.getMessage());
                e.printStackTrace();
            }
        });
    }

    public void produceMessage() throws JMSException {
        

        // Create connection, session, producer
        final Connection producerConnection = connectionFactory.createConnection();
        final Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        final Topic destination = producerSession.createTopic(topicName);
        final MessageProducer producer = producerSession.createProducer(null);

        // Prepare message
        final String text = "com.upwork.hermes.traffic.Producer:" + uid + " : " + count.incrementAndGet();
        final BytesMessage producerMessage = producerSession.createBytesMessage();
        producerMessage.setStringProperty("routingKey", routingKey);
        producerMessage.setLongProperty("timestamp", System.currentTimeMillis());
        producerMessage.setStringProperty("text", text);

        byte[] payload = new byte[1024 * 10];
        new Random().nextBytes(payload);
        producerMessage.writeBytes(payload);
        
        long start = System.currentTimeMillis();
        // Send message
        producer.send(destination, producerMessage, DeliveryMode.PERSISTENT, producer.getPriority(), producer.getTimeToLive());
        
        // Update metrics:
        long latency = System.currentTimeMillis() - start;
        timer.update(latency, TimeUnit.MILLISECONDS);
        
        // Close
        producer.close();
        producerSession.close();
        producerConnection.close();

        
    }

}
