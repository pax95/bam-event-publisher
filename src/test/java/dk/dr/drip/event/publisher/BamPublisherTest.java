package dk.dr.drip.event.publisher;

import org.junit.Test;

public class BamPublisherTest {

	@Test
	public void testBamPublisher() {
		BamPublisher bamPublisher = new BamPublisher();
		bamPublisher.setHost("bamtst01:7611");
		bamPublisher.setPassword("admin");
		bamPublisher.setUserName("admin");
		bamPublisher.publish("test", System.currentTimeMillis(), "123", 12000);
	}

}
