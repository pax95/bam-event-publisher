package dk.dr.drip.event.publisher;

public class DataBuilder {
	public static Object[] buildPayloadArray(String adapter, String msg, long timestamp, long msgSize) {
		return new Object[] { adapter, msg, msgSize, timestamp };
	}
}
