package edu.yu.cs.com3800;

import java.io.IOException;
import java.util.GregorianCalendar;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public interface LoggingServer {

    default Logger initializeLogging(String loggerName) throws IOException {
        Logger logger = Logger.getLogger(loggerName);
        logger.setUseParentHandlers(false);
        FileHandler fh = new FileHandler(loggerName, false);
        SimpleFormatter sf = new SimpleFormatter();
        fh.setFormatter(sf);
        logger.addHandler(fh);
        return logger;
    }

}
