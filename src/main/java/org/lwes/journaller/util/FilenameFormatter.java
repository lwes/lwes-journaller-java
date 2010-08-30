package org.lwes.journaller.util;
/**
 * @author fmaritato
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FilenameFormatter {

    private static transient Log log = LogFactory.getLog(FilenameFormatter.class);

    private static final String formatSpecifier =
            "%(\\d+\\$)?([-#+ 0,(\\<]*)?(\\d+)?(\\.\\d+)?([tT])?([a-zA-Z%])";
    private static Pattern fsPattern = Pattern.compile(formatSpecifier);

    public FilenameFormatter() {
    }


    public String format(String fmtString) {
        return format(fmtString, null);
    }

    public String format(String fmtString, Calendar c) {
        StringBuilder buf = new StringBuilder();
        List l = parse(fmtString, c);
        for (Object o : l) {
            buf.append(o.toString());
        }
        return buf.toString();
    }

    public List parse(String fmt) {
        return parse(fmt, null);
    }

    public List parse(String fmt, Calendar cal) {
        Calendar c;
        if (cal == null) {
            c = Calendar.getInstance();
        }
        else {
            c = cal;
        }
        LinkedList l = new LinkedList();
        if (fmt.indexOf('%') == -1) {
            l.add(fmt);
            return l;
        }
        Matcher m = fsPattern.matcher(fmt);
        int i = 0;
        while (i < fmt.length()) {
            if (m.find(i)) {
                if (m.start() != i) {
                    l.add(fmt.substring(i, m.start()));
                }
                String[] sa = new String[6];
                for (int j = 0; j < m.groupCount(); j++) {
                    sa[j] = m.group(j + 1);
                }
                // It is a time stamp field
                if ("t".equals(sa[4])) {
                    l.add(new DateTimeObject(sa[5].charAt(0), c));
                }
                // hostname
                else if ("h".equals(sa[5])) {
                    l.add(new HostnameObject());
                }
                else {
                    if (log.isDebugEnabled()) {
                        log.debug("unknown flag: " + sa[4]);
                    }
                }
            }
            i = m.end();
        }

        return l;
    }

    static class DateTimeObject {
        private char modifier;
        private Calendar calendar;

        public DateTimeObject(char modifier, Calendar calendar) {
            this.modifier = modifier;
            this.calendar = calendar;
        }

        /**
         * Only supported strings for now:
         * H - hour
         * M - minute
         * S - second
         * m - month
         * d - day of month
         * Y - 4 digit year
         *
         * @return
         */
        public String toString() {
            switch (modifier) {
                case DateTime.MILLISECOND:
                    return pad(calendar.get(Calendar.MILLISECOND));
                case DateTime.HOUR_OF_DAY_0:
                    return pad(calendar.get(Calendar.HOUR_OF_DAY));
                case DateTime.MINUTE:
                    return pad(calendar.get(Calendar.MINUTE));
                case DateTime.SECOND:
                    return pad(calendar.get(Calendar.SECOND));
                case DateTime.MONTH:
                    return pad(calendar.get(Calendar.MONTH) + 1);
                case DateTime.DAY_OF_MONTH_0:
                    return pad(calendar.get(Calendar.DAY_OF_MONTH));
                case DateTime.YEAR_4:
                    return "" + calendar.get(Calendar.YEAR);
                default:
                    return null;
            }
        }

        /**
         * Pads a number to 2 digits if necessary.
         *
         * @param val
         * @return
         */
        public String pad(int val) {
            if (val >= 0 && val < 10) {
                return "0" + val;
            }
            else {
                return "" + val;
            }
        }
    }

    static class HostnameObject {
        private char modifier;

        HostnameObject() {
        }

        public HostnameObject(char modifier) {
            this.modifier = modifier;
        }

        public String toString() {
            try {
                InetAddress addr = InetAddress.getLocalHost();
                return addr.getHostName();
            }
            catch (UnknownHostException e) {
            }
            return null;
        }
    }

    protected static class DateTime {
        static final char HOUR_OF_DAY_0 = 'H'; // (00 - 23)
        static final char HOUR_0 = 'I'; // (01 - 12)
        static final char HOUR_OF_DAY = 'k'; // (0 - 23) -- like H
        static final char HOUR = 'l'; // (1 - 12) -- like I
        static final char MINUTE = 'M'; // (00 - 59)
        static final char NANOSECOND = 'N'; // (000000000 - 999999999)
        static final char MILLISECOND = 'L'; // jdk, not in gnu (000 - 999)
        static final char MILLISECOND_SINCE_EPOCH = 'Q'; // (0 - 99...?)
        static final char AM_PM = 'p'; // (am or pm)
        static final char SECONDS_SINCE_EPOCH = 's'; // (0 - 99...?)
        static final char SECOND = 'S'; // (00 - 60 - leap second)
        static final char TIME = 'T'; // (24 hour hh:mm:ss)
        static final char ZONE_NUMERIC = 'z'; // (-1200 - +1200) - ls minus?
        static final char ZONE = 'Z'; // (symbol)

        // Date
        static final char NAME_OF_DAY_ABBREV = 'a'; // 'a'
        static final char NAME_OF_DAY = 'A'; // 'A'
        static final char NAME_OF_MONTH_ABBREV = 'b'; // 'b'
        static final char NAME_OF_MONTH = 'B'; // 'B'
        static final char CENTURY = 'C'; // (00 - 99)
        static final char DAY_OF_MONTH_0 = 'd'; // (01 - 31)
        static final char DAY_OF_MONTH = 'e'; // (1 - 31) -- like d
        static final char NAME_OF_MONTH_ABBREV_X = 'h'; // -- same b
        static final char DAY_OF_YEAR = 'j'; // (001 - 366)
        static final char MONTH = 'm'; // (01 - 12)
        static final char YEAR_2 = 'y'; // (00 - 99)
        static final char YEAR_4 = 'Y'; // (0000 - 9999)

        // Composites
        static final char TIME_12_HOUR = 'r'; // (hh:mm:ss [AP]M)
        static final char TIME_24_HOUR = 'R'; // (hh:mm same as %H:%M)
        static final char DATE_TIME = 'c';
        // (Sat Nov 04 12:02:33 EST 1999)
        static final char DATE = 'D'; // (mm/dd/yy)
        static final char ISO_STANDARD_DATE = 'F'; // (%Y-%m-%d)
    }
}
