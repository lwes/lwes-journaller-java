package org.lwes.journaller;

/**
 * @author fmaritato
 */
public interface JournallerConstants {

    public static final int MAX_HEADER_SIZE = 22;
    public static final int MAX_BODY_SIZE = 65513;
    public static final int MAX_MSG_SIZE = MAX_HEADER_SIZE + MAX_BODY_SIZE;

    static final String RECEIPT_TIME = "ReceiptTime";
    static final String SENDER_PORT = "SenderPort";
    static final String SENDER_IP = "SenderIP";
    static final String ENCODING = "enc";
    static final String SITE_ID = "SiteID";

}
