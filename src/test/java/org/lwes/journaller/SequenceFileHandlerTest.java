package org.lwes.journaller;
/**
 * User: fmaritato
 * Date: Apr 22, 2009
 */

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Test;
import org.lwes.EventSystemException;
import org.lwes.journaller.handler.SequenceFileHandler;

public class SequenceFileHandlerTest extends BaseJournallerTest {

    private transient Log log = LogFactory.getLog(SequenceFileHandlerTest.class);

    /**
     * "Emit" some events, make sure they end up in the file. Verify they can be read
     * and contain the fields that the journaller should be inserting.
     */
    @Test
    public void testHandler() throws IOException, EventSystemException {

        SequenceFileHandler handler = new SequenceFileHandler("target/junit-seq1",
                                                              "%tY%tm%td%tH%tM%tS%tL");
        handler.setTestTime(start);
        handler.setLastRotateTimestamp(last.getTimeInMillis());

        String generatedFile1 = handler.generateRotatedFilename(last, start);
        if (log.isDebugEnabled()) {
            log.debug("generated file: " + generatedFile1);
        }
        handler.handleEvent(createTestEvent());
        handler.handleEvent(createTestEvent());
        handler.destroy();

        MockSequenceDeJournaller mdj = new MockSequenceDeJournaller();
        mdj.setFileName(generatedFile1);
        mdj.run();

        verifyEvents(mdj);
    }

    @Test
    public void testRotation() throws IOException, EventSystemException, InterruptedException {
        SequenceFileHandler handler = new SequenceFileHandler("target/junit-seq2",
                                                              "%tY%tm%td%tH%tM%tS%tL");
        handler.setTestTime(start);
        handler.setLastRotateTimestamp(last.getTimeInMillis());

        String generatedFile1 = handler.generateRotatedFilename(last, start);
        if (log.isDebugEnabled()) {
            log.debug("generated file: " + generatedFile1);
        }

        handler.handleEvent(createTestEvent());
        handler.handleEvent(createTestEvent());
        Thread.sleep(1000);
        boolean rotated = handler.rotate();
        Assert.assertTrue(rotated);

        MockSequenceDeJournaller mdj = new MockSequenceDeJournaller();
        mdj.setFileName(generatedFile1);
        mdj.run();
        verifyEvents(mdj);

        String generatedFile2 = handler.generateRotatedFilename(start, start);
        if (log.isDebugEnabled()) {
            log.debug("generated file: " + generatedFile2);
        }
        handler.handleEvent(createTestEvent());
        handler.handleEvent(createTestEvent());
        handler.destroy();

        mdj.clear();
        mdj.setFileName(generatedFile2);
        mdj.run();

        verifyEvents(mdj);
    }
}
