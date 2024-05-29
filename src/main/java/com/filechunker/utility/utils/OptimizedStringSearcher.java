package com.filechunker.utility.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class OptimizedStringSearcher {

    private static final int NUM_THREADS = 8;

    public static void main(String[] args) throws InterruptedException, ExecutionException, IOException {
    	System.out.println("started at " + LocalDateTime.now());
        // Define the directory containing the chunked files and the strings to search for
        String directoryPath = "D:\\\\utils\\\\log_file_chunks\\\\";  // Update this path
        List<String> searchStrings = Arrays.asList("9884388086", "132", "3245");

        // Create a thread pool
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);

        try {
            // Get a list of all chunk files in the directory
            List<Path> chunkFiles = Files.list(Paths.get(directoryPath))
                    .filter(Files::isRegularFile)
                    .collect(Collectors.toList());

            // Prepare tasks for each file
            List<Callable<Map<String, Set<String>>>> tasks = new ArrayList<>();
            for (Path filePath : chunkFiles) {
                tasks.add(() -> searchStringsInFile(filePath, searchStrings));
            }

            // Invoke all tasks and collect the results
            List<Future<Map<String, Set<String>>>> results = executor.invokeAll(tasks);

            // Combine results from all tasks
            Map<String, Set<String>> combinedResults = new ConcurrentHashMap<>();
            for (String searchString : searchStrings) {
                combinedResults.put(searchString, ConcurrentHashMap.newKeySet());
            }

            for (Future<Map<String, Set<String>>> future : results) {
                Map<String, Set<String>> result = future.get();
                for (String searchString : searchStrings) {
                    combinedResults.get(searchString).addAll(result.get(searchString));
                }
            }

            // Print the results
            combinedResults.forEach((key, value) -> {
                System.out.println(key + " is present in: " + value);
            });
        } finally {
        	System.out.println("completed at " + LocalDateTime.now());
            // Shutdown the executor
            executor.shutdown();
        }
    }

    private static Map<String, Set<String>> searchStringsInFile(Path filePath, List<String> searchStrings) {
        Map<String, Set<String>> resultMap = new HashMap<>();
        for (String searchString : searchStrings) {
            resultMap.put(searchString, ConcurrentHashMap.newKeySet());
        }

        try (BufferedReader reader = Files.newBufferedReader(filePath)) {
            String line;
            while ((line = reader.readLine()) != null) {
                for (String searchString : searchStrings) {
                    if (line.contains(searchString)) {
                        resultMap.get(searchString).add(filePath.getFileName().toString());
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return resultMap;
    }
}
