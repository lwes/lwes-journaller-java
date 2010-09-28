package org.lwes.journaller;
/**
 * @author fmaritato
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kohsuke.args4j.CmdLineException;
import org.lwes.db.EventTemplateDB;
import org.lwes.journaller.util.EventHandlerUtil;
import org.lwes.serializer.DeserializerState;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

public class CountDeJournaller extends DeJournaller {
    private static transient Log log = LogFactory.getLog(CountDeJournaller.class);

    private int counter = 0;

    public void run() {
        if (fileName == null || "".equals(fileName)) {
            log.error("File name was not specified");
            return;
        }

        EventTemplateDB evtTemplate = new EventTemplateDB();
        evtTemplate.initialize();

        DeserializerState state = new DeserializerState();

        DataInputStream in = null;
        try {
            log.debug("Opening file: " + fileName);
            if (gzipped) {
                in = new DataInputStream(new GZIPInputStream(new FileInputStream(fileName)));
            }
            else {
                in = new DataInputStream(new FileInputStream(fileName));
            }
            byte[] bytes;
            while ((bytes = EventHandlerUtil.readEvent(in, state)) != null) {
                state.reset();
                handleEvent(bytes);
            }
        }
        catch (EOFException e) {
            // this is normal. Just catch and ignore.
        }
        catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        finally {
            if (in != null) {
                try {
                    in.close();
                }
                catch (IOException e) {
                    log.error(e.getMessage(), e);
                }
            }
            done();
        }
        log.info("Found: " + counter + " events.");
    }

    public void handleEvent(byte[] event) {
        counter++;
        if (log.isDebugEnabled()) {
            if (counter % 100 == 0) {
                log.debug("counter: " + counter);
            }
        }
    }

    public static void main(String[] args) {
        CountDeJournaller dj = new CountDeJournaller();
        try {
            dj.parseArguments(args);
        }
        catch (CmdLineException e) {
            log.error(e.getMessage(), e);
        }
        dj.run();
    }
}
