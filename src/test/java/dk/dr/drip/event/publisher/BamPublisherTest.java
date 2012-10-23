package dk.dr.drip.event.publisher;

import org.junit.Test;

public class BamPublisherTest {

	@Test
	public void testBamPublisher() {
		BamPublisher bamPublisher = new BamPublisher();
		bamPublisher.setHost("localhost:3333");
		bamPublisher.setPassword("bla");
		bamPublisher.setUserName("user");
		bamPublisher.publish("test", System.currentTimeMillis(), "123", 12000);
	}

}
