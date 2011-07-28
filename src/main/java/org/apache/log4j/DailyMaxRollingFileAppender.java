package org.apache.log4j;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * @author fmaritato
 */

public class DailyMaxRollingFileAppender extends DailyRollingFileAppender {

    /**
     * The maximum number of backup files to hold. The default is one.
     */
    protected int maxBackupIndex = 1;

    public DailyMaxRollingFileAppender() {
    }

    public DailyMaxRollingFileAppender(Layout layout,
                                       String filename,
                                       String datePattern) throws IOException {
        super(layout, filename, datePattern);
    }

    public int getMaxBackupIndex() {
        return maxBackupIndex;
    }

    public void setMaxBackupIndex(int maxBackupIndex) {
        this.maxBackupIndex = maxBackupIndex;
    }

    /**
     * Rollover according to how the DailyMaxRollingFileAppender would do it, then just delete
     * the leftover files defined by the maxBackupIndex variable. This assumes it is rotating the
     * files daily...
     *
     * @throws IOException
     */
    @Override
    void rollOver() throws IOException {

        super.rollOver();

        if (maxBackupIndex > 0) {
            int period = getPeriodicity();
            Calendar now = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            for (int i = maxBackupIndex + 1; ; i++) {
                now.add(period, -1 * i);
                File datedFile = new File(fileName + sdf.format(now.getTime()));
                if (datedFile.exists()) {
                    datedFile.delete();
                }
                else {
                    break;
                }
            }
        }
    }

    protected int getPeriodicity() {
        switch (computeCheckPeriod()) {
            case TOP_OF_MINUTE:
                return Calendar.MINUTE;
            case TOP_OF_HOUR:
                return Calendar.HOUR_OF_DAY;
            case HALF_DAY:
                return Calendar.HOUR_OF_DAY;
            case TOP_OF_DAY:
                return Calendar.DAY_OF_MONTH;
            case TOP_OF_WEEK:
                return Calendar.WEEK_OF_YEAR;
            case TOP_OF_MONTH:
                return Calendar.MONTH;
            default:
                return Calendar.DAY_OF_MONTH;
        }
    }
}