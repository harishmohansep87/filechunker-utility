package com.filechunker.utility.utils;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.util.concurrent.*;

public class LogFileSplitter {
    private static final long CHUNK_SIZE = 100 * 1024 * 1024; // 100 MB
    private static final String FILE_PATH = "D:\\utils\\large_log_file\\large_log_file.log";
    private static final String OUTPUT_PATH = "D:\\utils\\log_file_chunks\\";
    private static final int NUM_THREADS = 4; // Adjust based on your CPU cores

    public static void main(String[] args) {
    	System.out.println("started at : " + LocalDateTime.now());
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);

        try (FileChannel fileChannel = FileChannel.open(Paths.get(FILE_PATH), StandardOpenOption.READ)) {
            long fileSize = fileChannel.size();
            long position = 0;
            int count = 0;

            while (position < fileSize) {
                long remaining = fileSize - position;
                long size = Math.min(CHUNK_SIZE, remaining);

                final long currentPosition = position;
                final long currentSize = size;
                final int currentCount = count;

                executor.submit(() -> {
                    try (RandomAccessFile raf = new RandomAccessFile(FILE_PATH, "r");
                         FileChannel innerFileChannel = raf.getChannel()) {

                        MappedByteBuffer buffer = innerFileChannel.map(FileChannel.MapMode.READ_ONLY, currentPosition, currentSize);
                        byte[] data = new byte[(int) currentSize];
                        buffer.get(data);

                        Path outputPath = Paths.get(OUTPUT_PATH + "chunk_" + currentCount + ".log");
                        Files.write(outputPath, data);

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });

                position += size;
                count++;
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
        	System.out.println("completed at : " + LocalDateTime.now());
            executor.shutdown();
            try {
                if (!executor.awaitTermination(60, TimeUnit.MINUTES)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
            }
        }
    }
}
