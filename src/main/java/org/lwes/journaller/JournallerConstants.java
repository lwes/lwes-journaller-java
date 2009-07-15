package org.lwes.journaller;

/**
 * @author fmaritato
 */
public interface JournallerConstants {

    public static final int MAX_HEADER_SIZE = 22;
    public static final int MAX_BODY_SIZE = 65513;
    public static final int MAX_MSG_SIZE = MAX_HEADER_SIZE + MAX_BODY_SIZE;
    
}
