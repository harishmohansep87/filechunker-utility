package com.filechunker.utility.service;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class FileSplitterService {
	
	private static final Logger logger = LoggerFactory.getLogger(FileSplitterService.class);

    private static final long CHUNK_SIZE = 100 * 1024 * 1024; // 100 MB

    public void splitFile(String sourceFilePath) throws IOException {
        Path sourceFile = Paths.get(sourceFilePath);
        try (FileChannel fileChannel = FileChannel.open(sourceFile, StandardOpenOption.READ)) {
            long fileSize = fileChannel.size();
            long position = 0;
            int chunkNumber = 0;

            while (position < fileSize) {
                long chunkSize = Math.min(CHUNK_SIZE, fileSize - position);
                ByteBuffer buffer = ByteBuffer.allocate((int) chunkSize);
                fileChannel.read(buffer, position);

                buffer.flip();
                Path chunkFile = Paths.get(sourceFile.getParent().toString(), "chunk_" + chunkNumber + ".log");
                Files.write(chunkFile, buffer.array(), StandardOpenOption.CREATE);

                position += chunkSize;
                chunkNumber++;
            }
        }
        logger.info("completed at {}" , LocalDateTime.now());
    }
}
