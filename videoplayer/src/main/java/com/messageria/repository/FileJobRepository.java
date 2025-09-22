package com.messageria.repository;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;

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

    // opcional: listar all finished
    public synchronized Set<String> listFinished() {
        Set<String> out = new HashSet<>();
        File[] files = baseDir.toFile().listFiles((d, n) -> n.endsWith(".done"));
        if (files == null) return out;
        for (File f : files) out.add(f.getName().replace(".done", ""));
        return out;
    }
}
