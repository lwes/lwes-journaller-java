package org.lwes.journaller;
/**
 * @author fmaritato
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.kohsuke.args4j.CmdLineException;
import org.lwes.Event;
import org.lwes.EventSystemException;
import org.lwes.db.EventTemplateDB;

import java.io.IOException;

public class SequenceDejournaller extends DeJournaller {

    private static transient Log log = LogFactory.getLog(SequenceDejournaller.class);

    public void run() {
        Configuration conf = new Configuration();
        FileSystem fs = null;
        SequenceFile.Reader reader = null;
        try {
            if (log.isDebugEnabled()) {
                log.debug("Opening file: " + getFileName());
            }
            fs = FileSystem.get(conf);
            Path p = new Path(getFileName());
            reader = new SequenceFile.Reader(fs, p, conf);

            BytesWritable key = (BytesWritable) reader.getKeyClass().newInstance();
            NullWritable value = NullWritable.get();

            EventTemplateDB templ = new EventTemplateDB();
            while (reader.next(key, value)) {
                Event evt = new Event(key.getBytes(), false, templ);
                if (log.isDebugEnabled()) {
                    log.debug("read a k/v: "+evt.toOneLineString());
                }
                handleEvent(evt);
            }
        }
        catch (IOException e) {
            log.error(e.getMessage(), e);
        }
        catch (InstantiationException e) {
            log.error(e.getMessage(), e);
        }
        catch (IllegalAccessException e) {
            log.error(e.getMessage(), e);
        }
        catch (EventSystemException e) {
            log.error(e.getMessage(), e);
        }
        finally {
            if (reader != null) {
                try {
                    reader.close();
                }
                catch (IOException e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }

    public static void main(String[] args) {
        SequenceDejournaller dj = new SequenceDejournaller();
        try {
            dj.parseArguments(args);
        }
        catch (CmdLineException e) {
            log.error(e.getMessage(), e);
        }
        dj.run();
    }
}
