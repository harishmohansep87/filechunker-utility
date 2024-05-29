package com.filechunker.utility.service;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Service
public class StringSearchService {
	
	private static final Logger logger = LoggerFactory.getLogger(StringSearchService.class);

    private static final int NUM_THREADS = 8;
    private final ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);

    @Async
    public CompletableFuture<Map<String, Set<String>>> searchStringsInChunks(String directoryPath, List<String> searchStrings) throws InterruptedException, ExecutionException, IOException {
        List<Path> chunkFiles = Files.list(Paths.get(directoryPath))
                .filter(Files::isRegularFile)
                .collect(Collectors.toList());

        List<Callable<Map<String, Set<String>>>> tasks = new ArrayList<>();
        for (Path filePath : chunkFiles) {
            tasks.add(() -> searchStringsInFile(filePath, searchStrings));
        }

        List<Future<Map<String, Set<String>>>> results = executor.invokeAll(tasks);

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

        executor.shutdown();
        logger.info("search completed at {} " , LocalDateTime.now());
        return CompletableFuture.completedFuture(combinedResults);
    }

    private Map<String, Set<String>> searchStringsInFile(Path filePath, List<String> searchStrings) {
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
