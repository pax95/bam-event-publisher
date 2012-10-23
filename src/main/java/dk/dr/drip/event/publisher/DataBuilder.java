package dk.dr.drip.event.publisher;

public class DataBuilder {
	public static Object[] buildPayloadArray(String adapter, String msgId, long timestamp, long msgSize) {
		return new Object[] { adapter, msgId, msgSize, timestamp };
	}
}
