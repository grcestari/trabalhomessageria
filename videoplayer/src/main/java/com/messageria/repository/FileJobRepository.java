package com.messageria.repository;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class FileJobRepository {
    private final Path baseDir;

    public FileJobRepository(Path baseDir) throws IOException {
        this.baseDir = baseDir;
        if (!Files.exists(baseDir)) Files.createDirectories(baseDir);
    }

    public synchronized boolean isFinished(String jobId) {
        Path p = baseDir.resolve(jobId + ".done");
        return Files.exists(p);
    }

    public synchronized void markFinished(String jobId) throws IOException {
        Path p = baseDir.resolve(jobId + ".done");
        if (!Files.exists(p)) {
            Files.write(p, ("finished\n").getBytes(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        }
    }
}
