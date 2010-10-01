package org.lwes.journaller;
/**
 * @author fmaritato
 */

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

    private int counter = 0;

    public void run() {
        if (fileName == null || "".equals(fileName)) {
            System.err.println("File name was not specified");
            return;
        }

        EventTemplateDB evtTemplate = new EventTemplateDB();
        evtTemplate.initialize();

        DeserializerState state = new DeserializerState();

        DataInputStream in = null;
        try {
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
            e.printStackTrace();
        }
        finally {
            if (in != null) {
                try {
                    in.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
            done();
        }
        System.out.println("Found: " + counter + " events.");
    }

    public void handleEvent(byte[] event) {
        counter++;
    }

    public static void main(String[] args) {
        CountDeJournaller dj = new CountDeJournaller();
        try {
            dj.parseArguments(args);
        }
        catch (CmdLineException e) {
            e.printStackTrace();
        }
        dj.run();
    }
}
