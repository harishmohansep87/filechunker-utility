package com.filechunker.utility.controller;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.filechunker.utility.config.AppConfig;
import com.filechunker.utility.service.FileSplitterService;
import com.filechunker.utility.service.StringSearchService;

@RestController
@RequestMapping("/log")
public class LogController {
	
	private static final Logger logger = LoggerFactory.getLogger(LogController.class);
	
	@Autowired
	AppConfig appConfig;

    @Autowired
    private FileSplitterService fileSplitterService;

    @Autowired
    private StringSearchService stringSearchService;

    @PostMapping("/split")
    public void splitLogFile() throws IOException {
    	logger.info("started at {}" , LocalDateTime.now());
        fileSplitterService.splitFile(appConfig.getInputPath());
    }
    
    @GetMapping("/showFilePath")
    public String showFilePaths() throws Exception {
    	return "Input path: " + appConfig.getInputPath() + "Output path : " + appConfig.getOutputPath();
    }

    @PostMapping("/search")
    public Map<String, Set<String>> searchStringsInChunks(@RequestBody List<String> searchStrings) throws InterruptedException, ExecutionException, IOException {
    	logger.info("search started at {} " , LocalDateTime.now());
        return stringSearchService.searchStringsInChunks(appConfig.getOutputPath(), searchStrings).get();
    }
}
