package com.yis.util.shell;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeoutException;

/**
 * March, or die.
 *
 * @Created by yisany on 2020/01/07
 */
public class ShellUtil {

    private static final Logger logger = LogManager.getLogger(ShellUtil.class);
    private static long timeOut = 60 * 1000;

    public static String script(String... args) {
        try {
            long start = System.currentTimeMillis();
            ProcessBuilder pb = new ProcessBuilder(args);
            pb.redirectErrorStream(true);
            Process process = pb.start();

            Worker worker = new Worker(process);
            worker.start();
            ProcessStatus ps = worker.getProcessStatus();

            try {
                worker.join(timeOut);
                if (ps.exitCode == ProcessStatus.CODE_STARTED) {
                    // not finished
                    worker.interrupt();
                    throw new TimeoutException();
                } else {
                    long end = System.currentTimeMillis();
                    logger.info("script finish, status={}, cost={}ms", ps.exitCode, (end - start));
                    return ps.output;
                }
            } catch (TimeoutException e) {
                logger.error("ShellTaskJob.execute TimeoutException error, e={}", e);
                worker.interrupt();
                throw new RuntimeException("执行任务超时异常");
            }
        } catch (IOException e) {
            logger.error("ShellTaskJob.execute IOException error, e={}", e);
            throw new RuntimeException("执行任务IO异常");
        } catch (InterruptedException e) {
            logger.error("ShellTaskJob.execute InterruptedException error, e={}", e);
            throw new RuntimeException("执行任务异常");
        }

    }

    private static class Worker extends Thread {
        private final Process process;
        private ProcessStatus ps;

        private Worker(Process process) {
            this.process = process;
            this.ps = new ProcessStatus();
        }

        public void run() {
            try {
                InputStream is = process.getInputStream();
                try {
                    ps.output = IOUtils.toString(is);
                } catch (IOException ignore) {
                }
                ps.exitCode = process.waitFor();
            } catch (InterruptedException e) {
                logger.error("script execute InterruptedException, e={}", e);
                Thread.currentThread().interrupt();
            }
        }

        public ProcessStatus getProcessStatus() {
            return this.ps;
        }
    }

    private static class ProcessStatus {
        public static final int CODE_STARTED = -257;
        public volatile int exitCode;
        public volatile String output;
    }

}
