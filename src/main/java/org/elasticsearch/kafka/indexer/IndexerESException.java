/**
  * @author marinapopova
  * Sep 25, 2015
 */
package org.elasticsearch.kafka.indexer;

public class IndexerESException extends Exception {

	/**
	 * 
	 */
	public IndexerESException() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 */
	public IndexerESException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param cause
	 */
	public IndexerESException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 */
	public IndexerESException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public IndexerESException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		// TODO Auto-generated constructor stub
	}

}
