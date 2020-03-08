package com.upwork.hermes.traffic;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.jms.pool.PooledConnectionFactory;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TrafficApplication implements Runnable {

    public static final Object synchronised = new Object();
    public static Boolean shuttingDown = false;

    @CommandLine.Option(names = {"-e", "--endpoint"}, required = true, description = "broker endpoint")
    private String endpoint;

    @CommandLine.Option(names = {"-pp", "--ppassword"}, required = true, description = "producer password")
    private String producerPassword;

    @CommandLine.Option(names = {"-pu", "--puser"}, required = true, description = "producer username")
    private String producerUsername;

    @CommandLine.Option(names = {"-cp", "--cpassword"}, required = true, description = "consumer password")
    private String consumerPassword;

    @CommandLine.Option(names = {"-cu", "--cuser"}, required = true, description = "consumer username")
    private String consumerUsername;

    @CommandLine.Option(names = {"-t", "--topics"}, description = "Number of Virtual Topics")
    private int topics = 100;

    @CommandLine.Option(names = {"-q", "--queues"}, description = "Number of Queues per Virtual Topic")
    private int queues = 1;

    @CommandLine.Option(names = {"-c", "--consumers"}, description = "Number of Consumers per queue")
    private int consumers = 4;

    @CommandLine.Option(names = {"-cd", "--consumer-delay"}, description = "Consumers Delay, how long to process each message")
    private int consumerDelay = 100;

    @CommandLine.Option(names = {"-pd", "--producer-delay"}, description = "Producer Delay, rate is 1000/delay msg/sec per producer")
    private int producerDelay = 0;
    
    @CommandLine.Option(names = {"-si", "--start-index"}, description = "Start index for topics numbering")
    private int startIndex = 0;

    @Override
    public void run() {
        // Injector with traffic module
        final Injector injector = Guice.createInjector(new TrafficModule(endpoint, producerUsername, producerPassword, consumerUsername, consumerPassword));

        // Get instances
        final PooledConnectionFactory pooledConnectionFactory = injector.getInstance(PooledConnectionFactory.class);
        pooledConnectionFactory.start();
        final ActiveMQConnectionFactory consumerActiveMQConnectionFactory = injector.getInstance(Key.get(ActiveMQConnectionFactory.class, Names.named("consumer-connections")));
        final MetricRegistry metricRegistry = injector.getInstance(MetricRegistry.class);
        final ConsoleReporter consoleReporter = injector.getInstance(ConsoleReporter.class);
        consoleReporter.start(2, TimeUnit.SECONDS);

        final List<Subscriber> subscriberList = new ArrayList<>();
        final List<Producer> producers = new ArrayList<>();
        shutdownHook(pooledConnectionFactory, subscriberList, producers);

        final Timer producerTimer = metricRegistry.timer("producer-timer");
        final Timer consumerTimer = metricRegistry.timer("consumer-timer");
        final Timer dlqConsumerTimer = metricRegistry.timer("consumer-dlq-timer");

        // 1. Create producers, consumers
        for (int i = startIndex; i < startIndex + topics; i++) {
            synchronized (synchronised) {
                if (shuttingDown) {
                    return;
                }

                String topicName = String.format("VirtualTopic.publisher%03d", i);
                String routingKey = String.format("topic.%03d", i);
                System.out.println("Creating producer: " + routingKey + " -> " + topicName);
                final Producer producer = new Producer(routingKey, topicName, producerDelay, pooledConnectionFactory, producerTimer);
                producers.add(producer);

                for (int j = 0; j < queues; j++) {
                    String queueName = String.format("Consumer.subscriber%03d.%s", j, topicName);
                    String dlqQueueName = String.format("DLQ.Consumer.subscriber%03d.%s", j, topicName);

                    Set<String> routingKeySet = new HashSet<>();
                    routingKeySet.add(String.format("publisher%03d.topic.%03d", i, i));
                    System.out.println("Creating Subscriber: " + queueName + " -> " + routingKeySet.stream().collect(Collectors.joining(";")));
                    final Subscriber subscriber = new Subscriber(queueName, routingKeySet, consumers, consumerDelay, consumerActiveMQConnectionFactory, consumerTimer);
                    subscriberList.add(subscriber);
                    final Subscriber dlqSubscriber = new Subscriber(dlqQueueName, routingKeySet, consumers, consumerDelay, consumerActiveMQConnectionFactory, dlqConsumerTimer);
                    subscriberList.add(dlqSubscriber);
                }
            }
        }

        // 2. Start consumers first
        synchronized (synchronised) {
            for (Subscriber subscriber : subscriberList) {
                if (shuttingDown) {
                    return;
                }
                subscriber.run();
            }
        }

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // 3. Start producers
        synchronized (synchronised) {
            for (Producer producer : producers) {
                if (shuttingDown) {
                    return;
                }
                producer.run();
            }
        }
    }

    private static void shutdownHook(final PooledConnectionFactory pooledConnectionFactory, final List<Subscriber> subscriberList, final List<Producer> producers) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            shuttingDown = true;
            synchronized (synchronised) {
                producers.parallelStream().forEach(Producer::shutdown);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                subscriberList.parallelStream().forEach(Subscriber::shutdown);
                pooledConnectionFactory.stop();
            }

        }));
    }
}
