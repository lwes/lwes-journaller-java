package org.lwes.journaller;
/**
 * @author fmaritato
 */

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.lwes.db.EventTemplateDB;
import org.lwes.journaller.util.EventHandlerUtil;
import org.lwes.serializer.DeserializerState;

public class CountDeJournaller extends DeJournaller {

    private static final transient Log log = LogFactory.getLog(CountDeJournaller.class);

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
            if (sequence) {
                processSequenceFile(fileName);
            }
            else {
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

    protected void processSequenceFile(String file) {

        Configuration conf = new Configuration();
        FileSystem fs = null;
        SequenceFile.Reader reader = null;
        try {
            if (log.isDebugEnabled()) {
                log.debug("Opening file: " + getFileName());
            }
            fs = FileSystem.get(conf);
            Path p = new Path(file);
            reader = new SequenceFile.Reader(fs, p, conf);

            BytesWritable key = (BytesWritable) reader.getKeyClass().newInstance();
            BytesWritable value = (BytesWritable) reader.getKeyClass().newInstance();

            while (reader.next(key, value)) {
                handleEvent(key.getBytes());
            }
        }
        catch (InstantiationException e) {
            log.error(e.getMessage(), e);
        }
        catch (IllegalAccessException e) {
            log.error(e.getMessage(), e);
        }
        catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    public void handleEvent(byte[] event) {
        counter++;
    }

    public static void main(String[] args) {
        CountDeJournaller dj = new CountDeJournaller();
        CmdLineParser parser = null;
        try {
            parser = new CmdLineParser(dj);
            parser.parseArgument(args);
        }
        catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            return;
        }
        dj.run();
    }
}
