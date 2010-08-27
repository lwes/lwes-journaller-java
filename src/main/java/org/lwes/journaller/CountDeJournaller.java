package org.lwes.journaller;
/**
 * @author fmaritato
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lwes.Event;

public class CountDeJournaller extends DeJournaller {
    private static transient Log log = LogFactory.getLog(CountDeJournaller.class);

    private int counter = 0;

    public void run() {
        super.run();
        log.info("Found: " + counter + " events.");
    }

    @Override
    public void handleEvent(Event event) {
        counter++;
        if (log.isDebugEnabled()) {
            if (counter % 100 == 0) {
                log.debug("counter: " + counter);
            }
        }
    }

    public static void main(String[] args) {
        CountDeJournaller dj = new CountDeJournaller();
        dj.run();
    }
}
