package com.filechunker.utility.utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class LargeLogFileGenerator {

    private static final long FILE_SIZE_IN_BYTES = 25L * 1024 * 1024 * 1024; // 25 GB
    private static final String FILE_PATH = "D:\\utils\\large_log_file\\large_log_file.log";
    private static final String[] SAMPLE_LOG_ENTRIES = {
            "INFO 2024-05-29 12:00:00 Sample log entry 1",
            "WARN 2024-05-29 12:01:00 Sample log entry 2",
            "ERROR 2024-05-29 12:02:00 Sample log entry 3",
            "DEBUG 2024-05-29 12:03:00 Sample log entry 4"
    };
    private static final int NUM_GENERATOR_THREADS = 25;
    private static final int QUEUE_CAPACITY = 1000;

    public static void main(String[] args) {
        BlockingQueue<String> logQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
        ExecutorService executor = Executors.newFixedThreadPool(NUM_GENERATOR_THREADS);

        // Start log entry generator threads
        for (int i = 0; i < NUM_GENERATOR_THREADS; i++) {
            executor.submit(new LogEntryGenerator(logQueue));
        }

        // Start the log entry writer thread
        Thread writerThread = new Thread(new LogEntryWriter(logQueue));
        writerThread.start();

        // Shutdown generator threads
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Signal the writer thread to finish and wait for it to complete
        writerThread.interrupt();
        try {
            writerThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("25GB log file created successfully.");
    }

    static class LogEntryGenerator implements Runnable {
        private final BlockingQueue<String> logQueue;
        private final Random random = new Random();

        LogEntryGenerator(BlockingQueue<String> logQueue) {
            this.logQueue = logQueue;
        }

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                String logEntry = SAMPLE_LOG_ENTRIES[random.nextInt(SAMPLE_LOG_ENTRIES.length)] + "\n";
                try {
                    logQueue.put(logEntry);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    static class LogEntryWriter implements Runnable {
        private final BlockingQueue<String> logQueue;

        LogEntryWriter(BlockingQueue<String> logQueue) {
            this.logQueue = logQueue;
        }

        @Override
        public void run() {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(FILE_PATH))) {
                long currentSize = 0;
                while (currentSize < FILE_SIZE_IN_BYTES) {
                    String logEntry = logQueue.poll(1, TimeUnit.SECONDS);
                    if (logEntry != null) {
                        writer.write(logEntry);
                        currentSize += logEntry.getBytes().length;
                    }
                }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
