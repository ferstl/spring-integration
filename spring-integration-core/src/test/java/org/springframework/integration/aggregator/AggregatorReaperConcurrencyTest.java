package org.springframework.integration.aggregator;

import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.integration.Message;
import org.springframework.integration.MessageChannel;
import org.springframework.integration.support.MessageBuilder;

import static org.junit.Assert.*;

/**
 * Unit test that tries to reveal a concurrency issue in {@link AbstractCorrelatingMessageHandler}.
 */
public class AggregatorReaperConcurrencyTest {

	private static final int POOL_SIZE = 4;
	
	@Test
	@SuppressWarnings("unchecked")
	public void test() {
		ClassPathXmlApplicationContext ctx =
				new ClassPathXmlApplicationContext("AggregatorReaperConcurrencyTest-context.xml", this.getClass());
		
		MessageChannel inputChannel = ctx.getBean("inputChannel", MessageChannel.class);
		Queue<Message<Collection<Integer>>> queue = ctx.getBean("outputQueue", Queue.class);
		Queue<Message<?>> discardQueue = ctx.getBean("discardQueue", Queue.class);
		
		CountDownLatch latch = new CountDownLatch(1);
		ExecutorService pool = Executors.newFixedThreadPool(POOL_SIZE);
		for (int i = 0; i < POOL_SIZE; i++) {
			pool.execute(new SendTask(i * SendTask.ITERATIONS, inputChannel, latch));
		}
		
		latch.countDown();
		pool.shutdown();
		try {
			pool.awaitTermination(1, TimeUnit.MINUTES);
			// Wait a bit in case a reaper is still expiring messages
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		
		int messageCount = 0;
		for (Message<Collection<Integer>> message : queue) {
			messageCount += message.getPayload().size();
		}
		
		assertEquals(POOL_SIZE * SendTask.ITERATIONS, messageCount);
		assertEquals(0, discardQueue.size());
	}
	
	static class SimpleCorrelationStrategy implements CorrelationStrategy {
		public Object getCorrelationKey(Message<?> message) {
			return "simpleCorrelation";
		}
	}
	
	static class SendTask implements Runnable {

		public static final int ITERATIONS = 100000;
		
		private final int start;
		private final MessageChannel channel;
		private final CountDownLatch latch;
		
		public SendTask(int start, MessageChannel channel, CountDownLatch latch) {
			this.start = start;
			this.channel = channel;
			this.latch = latch;
		}

		public void run() {
			try {
				latch.await();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
			for (int i = start; i < start + ITERATIONS; i++) {
				channel.send(MessageBuilder.withPayload(i).build());
			}
		}
		
	}

}
