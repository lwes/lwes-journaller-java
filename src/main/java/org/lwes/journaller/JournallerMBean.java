package org.lwes.journaller;

import java.io.IOException;

/**
 * @author fmaritato
 */
public interface JournallerMBean {

    public boolean rotate() throws IOException;
}
