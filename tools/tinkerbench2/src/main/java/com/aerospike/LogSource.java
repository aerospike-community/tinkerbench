package com.aerospike;


import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.javatuples.Pair;

import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.PrintStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public final class LogSource {

    private static LogSource instance;

    public final static DateTimeFormatter DateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private final static AtomicInteger debugCnt = new AtomicInteger(0);
    private final boolean debugEnabled;

    private final org.slf4j.Logger logger4j;

    public org.slf4j.Logger getLogger4j() { return logger4j; }
    public boolean isDebug() { return debugEnabled; }

    public LogSource(boolean debugEnabled) {
        this.debugEnabled = debugEnabled;
        this.logger4j = LoggerFactory.getLogger("com.aerospike.tinkerbench2");
        instance = this;
    }

    public static LogSource getInstance() {
        if (instance == null) { // Check if instance already exists
            instance = new LogSource(false); // Create instance if not
        }

        return instance; // Return the single instance
    }

    public Boolean loggingEnabled() { return debugEnabled || logger4j.isErrorEnabled(); }

    public int getDebugCnt() { return debugCnt.get(); }

    public final static class Stream implements Closeable {

        private final LogSource source;
        private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        private final PrintStream printStream = new PrintStream(outputStream);

        public Stream(LogSource source) { this.source = source; }
        public Stream() { this.source = LogSource.getInstance(); }

        public PrintStream getPrintStream() { return printStream; }

        @Override
        public String toString() { return outputStream.toString(); }

        public void info() { source.info(toString()); }
        public void debug() { source.getLogger4j().debug(toString()); }
        public void warn() { source.getLogger4j().warn(toString()); }
        public void error() { source.getLogger4j().error(toString()); }
        public void trace() { source.getLogger4j().trace(toString()); }
        public void Print(String name, boolean err) { source.Print(name, err, toString()); }
        public void PrintDebug(String name) { source.PrintDebug(name, toString()); }

        @Override
        public void close() {
            try {
                printStream.close();
                outputStream.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void Print(String name, String msg, boolean err, boolean limited) {

        if (limited) {
            if (debugCnt.incrementAndGet() <= 1) {
                PrintDebug(name, "LIMIT1 " + msg, err);
            } else if (debugCnt.compareAndSet(100, 1)) {
                PrintDebug(name, "LIMIT100 " + msg, err);
            }
        } else {
            PrintDebug(name, msg, err);
        }
    }

    public void Print(String name, String msg, boolean err) {

        LocalDateTime now = LocalDateTime.now();
        String formattedDateTime = now.format(DateFormatter);
        Pair<?,?> pidThread = Helpers.GetPidThreadId();
        String fmtMsg = String.format("%s INFO %s %s %s %s%n",
                                        formattedDateTime,
                                        pidThread.getValue0(),
                                        pidThread.getValue1(),
                                        name,
                                        msg)
                            .replace("%", "%%");

        if(err) {
            System.err.printf(fmtMsg);
        }
        else {
            System.out.printf(fmtMsg);
        }
    }

    public void Print(String name, Throwable ex) {

        LocalDateTime now = LocalDateTime.now();
        String formattedDateTime = now.format(DateFormatter);
        Pair<?,?> pidThread = Helpers.GetPidThreadId();
        String errMsg = ex.getMessage();
        if(errMsg == null || errMsg.isEmpty()) {
            errMsg = "<Missing Message>";
        }
        String fmtMsg = String.format("%s ERROR %s %s %s %s: %s%n",
                                        formattedDateTime,
                                        pidThread.getValue0(),
                                        pidThread.getValue1(),
                                        name,
                                        ex.getClass().getSimpleName(),
                                        errMsg)
                            .replace("%","%%");
        System.err.printf(fmtMsg);
        if(isDebug()) {
            ex.printStackTrace(System.err);
            if(ex instanceof ResponseException) {
                ResponseException re = (ResponseException) ex;
                System.err.printf("\tStatus Code: %s%n\t\tRemote: %s%n\t\tRemote Stack Trace: %s%n",
                        re.getResponseStatusCode(),
                        re.getRemoteExceptionHierarchy(),
                        re.getRemoteStackTrace());
            }
        }
    }

    public void Print(String name, boolean err, String msg, Object var2) {
        Print(name, String.format(msg, var2), err);
    }

    public void Print(String name, boolean err, String msg, Object var2, Object var3) {
       PrintDebug(name, String.format(msg, err, var2, var3), err);
    }

    public void Print(String name, boolean err, String msg, Object... var2) {
        Print(name, String.format(msg, var2), err);
    }

    public void PrintDebug(String name, String msg, boolean limited) {

        if(debugEnabled) {
            if (limited) {
                if (debugCnt.incrementAndGet() <= 1) {
                    PrintDebug(name, "LIMIT1 " + msg);
                } else if (debugCnt.compareAndSet(100, 1)) {
                    PrintDebug(name, "LIMIT100 " + msg);
                }
            } else {
                PrintDebug(name, msg);
            }
        }
    }

    public void PrintDebug(String name, String msg) {
        if(debugEnabled) {
            LocalDateTime now = LocalDateTime.now();
            String formattedDateTime = now.format(DateFormatter);
            Pair<?,?> pidThread = Helpers.GetPidThreadId();
            String fmtMsg = String.format("%s DEBUG %s %s %s %s%n",
                            formattedDateTime,
                            pidThread.getValue0(),
                            pidThread.getValue1(),
                            name,
                            msg)
                            .replace("%", "%%");

            System.out.printf(fmtMsg);
            logger4j.debug(fmtMsg);
        }
    }

    public void PrintDebug(String name, Throwable ex) {
        if(debugEnabled) {
            LocalDateTime now = LocalDateTime.now();
            String formattedDateTime = now.format(DateFormatter);
            Pair<?,?> pidThread = Helpers.GetPidThreadId();
            String errMsg = ex.getMessage();
            if(errMsg == null || errMsg.isEmpty()) {
                errMsg = "<Missing Message>";
            }
            String fmtMsg = String.format("%s DEBUG %s %s %s %s: %s%n",
                                            formattedDateTime,
                                            pidThread.getValue0(),
                                            pidThread.getValue1(),
                                            name,
                                            ex.getClass().getSimpleName(),
                                            errMsg)
                            .replace("%","%%");
            System.err.printf(fmtMsg);
            ex.printStackTrace(System.err);

            if(ex instanceof ResponseException) {
                ResponseException re = (ResponseException) ex;
                System.err.printf("\tStatus Code: %s%n\t\tRemote: %s%n\t\tRemote Stack Trace: %s%n",
                                    re.getResponseStatusCode(),
                                    re.getRemoteExceptionHierarchy(),
                                    re.getRemoteStackTrace());
            }
        }
    }

    public void PrintDebug(String name, String msg, Object var2) {
        if(debugEnabled) {
            PrintDebug(name, String.format(msg, var2));
        }
    }

    public void PrintDebug(String name, String msg, Object var2, Object var3) {
        if(debugEnabled) {
            PrintDebug(name, String.format(msg, var2, var3));
        }
    }

    public void PrintDebug(String name, String msg, Object... var2) {
        if(debugEnabled) {
            PrintDebug(name, String.format(msg, var2));
        }
    }

    public void title(TinkerBench2Args args) {

        logger4j.info("==============> Starting <==============");
        StringBuilder argStr = new StringBuilder("Arguments:\n");
        for (String arg : args.getArguments(false)) {
            argStr.append("\t")
                    .append(arg)
                    .append("\n");
        }
        logger4j.info(argStr.toString());
        argStr = new StringBuilder("Versions:\n");
        for(String version : args.getVersions(false)) {
            argStr.append("\t")
                    .append(version)
                    .append("\n");
        }
        logger4j.info(argStr.toString());
    }

    public void info(String msg) { logger4j.info(msg); }

    public void info(String var1, Object var2) { logger4j.info(var1, var2); }

    public void info(String var1, Object var2, Object var3) { logger4j.info(var1, var2, var3); }

    public void info(String var1, Object... var2) { logger4j.info(var1, var2); }

    public void info(String var1, Throwable var2) { logger4j.info(var1, var2); }

    public void warn(String msg) { logger4j.warn(msg); }

    public void warn(String var1, Object var2) { logger4j.warn(var1, var2); }

    public void warn(String var1, Object var2, Object var3) { logger4j.warn(var1, var2, var3); }

    public void warn(String var1, Object... var2) { logger4j.warn(var1, var2); }

    public void warn(String var1, Throwable var2) { logger4j.warn(var1, var2); }

    public void error(String msg) { logger4j.error(msg); }

    public void error(String var1, Object var2) { logger4j.error(var1, var2); }

    public void error(String var1, Object var2, Object var3) { logger4j.error(var1, var2, var3); }

    public void error(String var1, Object... var2) { logger4j.error(var1, var2); }

    public void error(String var1, Throwable var2) { logger4j.error(var1, var2); }

}
