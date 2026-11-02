package task5;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class LogAnalysisAssignment {

    public record LogEntry(
            String method,
            String path,
            int status,
            long responseBytes,
            long durationMillis
    ) {}

    public record LogFile(Path path) {}

    public record PathStatistics(
            long requests,
            long successful,
            long errors,
            long responseBytes,
            long totalDurationMillis,
            long maxDurationMillis
    ) {}

    public record Report(
            long totalRequests,
            long successfulRequests,
            long errorRequests,
            long responseBytes,
            long maxDurationMillis
    ) {}

    static class Result {
        Report report;
        Map<String, PathStatistics> perPath;
        long okFiles;
        long badFiles;
        long totalLines;
        long validLines;
        long badLines;
        long readErr;
        int maxActive;
        boolean stoppedEarly;
    }

    static volatile boolean stopSignal = false;

    static LogEntry parseLine(String line) {
        if (line == null) {
            throw new IllegalArgumentException("null");
        }
        String trimmed = line.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("empty");
        }
        String[] parts = trimmed.split("\\s+");
        if (parts.length != 5) {
            throw new IllegalArgumentException(
                    "expected 5 fields, got " + parts.length + ": [" + line + "]");
        }
        try {
            return new LogEntry(
                    parts[0],
                    parts[1],
                    Integer.parseInt(parts[2]),
                    Long.parseLong(parts[3]),
                    Long.parseLong(parts[4])
            );
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("not a number: [" + line + "]", e);
        }
    }

    static class Stats {
        long requests;
        long successful;
        long errors;
        long responseBytes;
        long totalDuration;
        long maxDuration;

        void add(LogEntry e) {
            requests++;
            if (e.status() >= 200 && e.status() < 400) {
                successful++;
            } else if (e.status() >= 400) {
                errors++;
            }
            responseBytes += e.responseBytes();
            totalDuration += e.durationMillis();
            if (e.durationMillis() > maxDuration) {
                maxDuration = e.durationMillis();
            }
        }

        PathStatistics snapshot() {
            return new PathStatistics(requests, successful, errors,
                    responseBytes, totalDuration, maxDuration);
        }
    }

    static void bumpMax(AtomicInteger maxRef, int value) {
        int cur;
        while (value > (cur = maxRef.get())) {
            if (maxRef.compareAndSet(cur, value)) return;
        }
    }

    static Report buildReport(Map<String, Stats> byPath) {
        long req = 0, succ = 0, err = 0, bytes = 0, maxDur = 0;
        for (Stats s : byPath.values()) {
            req   += s.requests;
            succ  += s.successful;
            err   += s.errors;
            bytes += s.responseBytes;
            if (s.maxDuration > maxDur) maxDur = s.maxDuration;
        }
        return new Report(req, succ, err, bytes, maxDur);
    }

    static Map<String, PathStatistics> snapshot(Map<String, Stats> byPath) {
        Map<String, PathStatistics> result = new HashMap<>();
        for (var e : byPath.entrySet()) {
            result.put(e.getKey(), e.getValue().snapshot());
        }
        return result;
    }

    static Result analyzeSequential(List<LogFile> files) {
        Map<String, Stats> byPath = new HashMap<>();
        long okFiles = 0, badFiles = 0;
        long totalLines = 0, validLines = 0, badLines = 0, readErr = 0;

        for (LogFile f : files) {
            try {
                Iterator<String> it = LogFileLines.iterator(f.path());
                while (it.hasNext()) {
                    String line = it.next();
                    totalLines++;
                    try {
                        LogEntry entry = parseLine(line);
                        Stats s = byPath.computeIfAbsent(entry.path(), k -> new Stats());
                        s.add(entry);
                        validLines++;
                    } catch (IllegalArgumentException ex) {
                        badLines++;
                    }
                }
                okFiles++;
            } catch (RuntimeException ex) {
                badFiles++;
                readErr++;
            }
        }

        Result r = new Result();
        r.report = buildReport(byPath);
        r.perPath = snapshot(byPath);
        r.okFiles = okFiles;
        r.badFiles = badFiles;
        r.totalLines = totalLines;
        r.validLines = validLines;
        r.badLines = badLines;
        r.readErr = readErr;
        r.maxActive = files.isEmpty() ? 0 : 1;
        r.stoppedEarly = false;
        return r;
    }

    static Result analyzeGlobal(List<LogFile> files, int poolSize, int sem)
            throws InterruptedException {

        ExecutorService pool = Executors.newFixedThreadPool(poolSize);
        Semaphore semaphore = new Semaphore(sem);
        Map<String, Stats> byPath = new HashMap<>();
        Object globalLock = new Object();

        AtomicLong okFiles    = new AtomicLong();
        AtomicLong badFiles   = new AtomicLong();
        AtomicLong totalLines = new AtomicLong();
        AtomicLong validLines = new AtomicLong();
        AtomicLong badLines   = new AtomicLong();
        AtomicLong readErr    = new AtomicLong();
        AtomicInteger activeFiles = new AtomicInteger();
        AtomicInteger maxActive   = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(files.size());

        for (LogFile f : files) {
            pool.execute(() -> {
                boolean acquired = false;
                try {
                    semaphore.acquire();
                    acquired = true;

                    bumpMax(maxActive, activeFiles.incrementAndGet());

                    try {
                        Iterator<String> it = LogFileLines.iterator(f.path());
                        while (it.hasNext()) {
                            if (stopSignal) return;
                            String line = it.next();
                            totalLines.incrementAndGet();
                            try {
                                LogEntry entry = parseLine(line);
                                synchronized (globalLock) {
                                    Stats s = byPath.computeIfAbsent(
                                            entry.path(), k -> new Stats());
                                    s.add(entry);
                                }
                                validLines.incrementAndGet();
                            } catch (IllegalArgumentException ex) {
                                badLines.incrementAndGet();
                            }
                        }
                        okFiles.incrementAndGet();
                    } catch (RuntimeException ex) {
                        ex.printStackTrace();
                        badFiles.incrementAndGet();
                        readErr.incrementAndGet();
                    } finally {
                        activeFiles.decrementAndGet();
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    badFiles.incrementAndGet();
                } finally {
                    if (acquired) semaphore.release();
                    latch.countDown();
                }
            });
        }

        latch.await();
        pool.shutdown();
        if (!pool.awaitTermination(1, TimeUnit.MINUTES)) {
            pool.shutdownNow();
        }

        Result r = new Result();
        r.report = buildReport(byPath);
        r.perPath = snapshot(byPath);
        r.okFiles = okFiles.get();
        r.badFiles = badFiles.get();
        r.totalLines = totalLines.get();
        r.validLines = validLines.get();
        r.badLines = badLines.get();
        r.readErr = readErr.get();
        r.maxActive = maxActive.get();
        r.stoppedEarly = stopSignal;
        return r;
    }

    static Result analyzeParallel(List<LogFile> files, int poolSize, int sem)
            throws InterruptedException {

        ExecutorService pool = Executors.newFixedThreadPool(poolSize);
        Semaphore semaphore = new Semaphore(sem);
        ConcurrentHashMap<String, Stats> byPath = new ConcurrentHashMap<>();

        AtomicLong okFiles    = new AtomicLong();
        AtomicLong badFiles   = new AtomicLong();
        AtomicLong totalLines = new AtomicLong();
        AtomicLong validLines = new AtomicLong();
        AtomicLong badLines   = new AtomicLong();
        AtomicLong readErr    = new AtomicLong();
        AtomicInteger activeFiles = new AtomicInteger();
        AtomicInteger maxActive   = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(files.size());

        for (LogFile f : files) {
            pool.execute(() -> {
                boolean acquired = false;
                try {
                    semaphore.acquire();
                    acquired = true;

                    bumpMax(maxActive, activeFiles.incrementAndGet());

                    try {
                        Iterator<String> it = LogFileLines.iterator(f.path());
                        while (it.hasNext()) {
                            if (stopSignal) return;
                            String line = it.next();
                            totalLines.incrementAndGet();
                            try {
                                LogEntry entry = parseLine(line);
                                Stats s = byPath.computeIfAbsent(
                                        entry.path(), k -> new Stats());
                                synchronized (s) {
                                    s.add(entry);
                                }
                                validLines.incrementAndGet();
                            } catch (IllegalArgumentException ex) {
                                badLines.incrementAndGet();
                            }
                        }
                        okFiles.incrementAndGet();
                    } catch (RuntimeException ex) {
                        ex.printStackTrace();
                        badFiles.incrementAndGet();
                        readErr.incrementAndGet();
                    } finally {
                        activeFiles.decrementAndGet();
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    badFiles.incrementAndGet();
                } finally {
                    if (acquired) semaphore.release();
                    latch.countDown();
                }
            });
        }

        latch.await();
        pool.shutdown();
        if (!pool.awaitTermination(1, TimeUnit.MINUTES)) {
            pool.shutdownNow();
        }

        Result r = new Result();
        r.report = buildReport(byPath);
        r.perPath = snapshot(byPath);
        r.okFiles = okFiles.get();
        r.badFiles = badFiles.get();
        r.totalLines = totalLines.get();
        r.validLines = validLines.get();
        r.badLines = badLines.get();
        r.readErr = readErr.get();
        r.maxActive = maxActive.get();
        r.stoppedEarly = stopSignal;
        return r;
    }

    static void checkInvariants(Result r, int sem) {
        for (var e : r.perPath.entrySet()) {
            PathStatistics ps = e.getValue();
            if (ps.requests() != ps.successful() + ps.errors()) {
                System.out.println("BUG: requests != success+errors for " + e.getKey());
            }
            if (ps.requests() > 0 && ps.maxDurationMillis() > ps.totalDurationMillis()) {
                System.out.println("BUG: maxDuration > totalDuration for " + e.getKey());
            }
        }
        if (r.totalLines != r.validLines + r.badLines) {
            System.out.println("BUG: totalLines != valid+bad");
        }
        if (r.maxActive > sem) {
            System.out.println("BUG: activeFiles > semLimit: " + r.maxActive + " > " + sem);
        }
        System.out.println("Инварианты проверены.");
    }

    static List<LogFile> listLogFiles(Path dir) throws IOException {
        List<LogFile> result = new ArrayList<>();
        try (var stream = Files.list(dir)) {
            for (Path p : stream.toList()) {
                if (Files.isRegularFile(p)
                        && p.getFileName().toString().endsWith(".log")) {
                    result.add(new LogFile(p));
                }
            }
        }
        Collections.sort(result, (a, b) -> a.path().compareTo(b.path()));
        return result;
    }

    static void printDiagnostics(String label, long ms, Result r) {
        System.out.println("=== " + label + " (" + ms + " ms) ===");
        System.out.println("  файлов ок: " + r.okFiles + ", битых: " + r.badFiles);
        System.out.println("  строк: всего " + r.totalLines
                + ", валидных " + r.validLines
                + ", битых " + r.badLines
                + ", ошибок чтения " + r.readErr);
        System.out.println("  активных файлов максимум: " + r.maxActive);
        System.out.println("  остановлено досрочно: " + r.stoppedEarly);
        System.out.println("  по URL:");
        for (var e : r.perPath.entrySet()) {
            PathStatistics ps = e.getValue();
            System.out.println("    " + e.getKey()
                    + ": req=" + ps.requests()
                    + ", ok=" + ps.successful()
                    + ", err=" + ps.errors()
                    + ", bytes=" + ps.responseBytes()
                    + ", totalDur=" + ps.totalDurationMillis()
                    + ", maxDur=" + ps.maxDurationMillis());
        }
        Report rep = r.report;
        System.out.println("  отчёт: totalReq=" + rep.totalRequests()
                + ", ok=" + rep.successfulRequests()
                + ", err=" + rep.errorRequests()
                + ", bytes=" + rep.responseBytes()
                + ", maxDur=" + rep.maxDurationMillis());
    }

    public static final class LogFileLines {
        private LogFileLines() {}

        public static Iterator<String> iterator(Path path) {
            try {
                BufferedReader reader = Files.newBufferedReader(
                        path,
                        StandardCharsets.UTF_8
                );

                return new Iterator<>() {
                    private String nextLine = readNext();
                    private boolean closed;

                    private String readNext() {
                        try {
                            String line = reader.readLine();
                            if (line == null) {
                                close();
                            }
                            return line;
                        } catch (IOException exception) {
                            close();
                            throw new RuntimeException(
                                    "Cannot read " + path,
                                    exception
                            );
                        }
                    }

                    private void close() {
                        if (closed) {
                            return;
                        }
                        closed = true;
                        try {
                            reader.close();
                        } catch (IOException exception) {
                            throw new RuntimeException(
                                    "Cannot close " + path,
                                    exception
                            );
                        }
                    }

                    @Override
                    public boolean hasNext() {
                        return nextLine != null;
                    }

                    @Override
                    public String next() {
                        if (nextLine == null) {
                            throw new NoSuchElementException();
                        }
                        String result = nextLine;
                        nextLine = readNext();
                        return result;
                    }
                };
            } catch (IOException exception) {
                throw new RuntimeException(
                        "Cannot open " + path,
                        exception
                );
            }
        }
    }

    public static final class GenerateLogs {
        private static final String[] METHODS = {
                "GET", "POST", "PUT", "DELETE"
        };
        private static final String[] PATHS = {
                "/", "/index.html", "/login", "/api/users",
                "/api/orders", "/api/products", "/search", "/health"
        };
        private static final int[] STATUSES = {
                200, 200, 200, 201, 204,
                301, 400, 401, 404, 500
        };

        private GenerateLogs() {}

        public static void generate(
                Path outputDirectory,
                int fileCount,
                int linesPerFile
        ) throws IOException {
            if (fileCount < 0 || linesPerFile < 0) {
                throw new IllegalArgumentException(
                        "Counts must be non-negative"
                );
            }

            Files.createDirectories(outputDirectory);
            Random random = new Random(42);

            for (int fileId = 0; fileId < fileCount; fileId++) {
                Path file = outputDirectory.resolve(
                        "access-%03d.log".formatted(fileId)
                );

                try (BufferedWriter writer = Files.newBufferedWriter(
                        file,
                        StandardCharsets.UTF_8
                )) {
                    for (int line = 0; line < linesPerFile; line++) {
                        String method =
                                METHODS[random.nextInt(METHODS.length)];
                        String path =
                                PATHS[random.nextInt(PATHS.length)];
                        int status =
                                STATUSES[random.nextInt(STATUSES.length)];
                        long responseBytes =
                                100L + random.nextInt(100_000);
                        long durationMillis =
                                1L + random.nextInt(500);

                        writer.write(
                                "%s %s %d %d %d%n".formatted(
                                        method,
                                        path,
                                        status,
                                        responseBytes,
                                        durationMillis
                                )
                        );
                    }
                }
            }
        }
    }

    private static void selfCheck() throws IOException {
        Path directory = Files.createTempDirectory("log-assignment-");
        GenerateLogs.generate(directory, 3, 7);

        int files = 0;
        int lines = 0;

        try (var paths = Files.list(directory)) {
            for (Path path : paths.toList()) {
                files++;
                Iterator<String> iterator = LogFileLines.iterator(path);

                while (iterator.hasNext()) {
                    String line = iterator.next();
                    String[] fields = line.trim().split("\\s+");
                    if (fields.length != 5) {
                        throw new AssertionError(
                                "Unexpected line: " + line
                        );
                    }
                    Integer.parseInt(fields[2]);
                    Long.parseLong(fields[3]);
                    Long.parseLong(fields[4]);
                    lines++;
                }
            }
        }

        if (files != 3 || lines != 21) {
            throw new AssertionError(
                    "Expected 3 files and 21 lines, got "
                            + files + " files and " + lines + " lines"
            );
        }

        System.out.println(
                "OK: generated " + files
                        + " files and read " + lines + " lines"
        );
    }

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].equals("generate")) {
            Path directory = Path.of(
                    args.length > 1 ? args[1] : "logs"
            );
            int fileCount = args.length > 2
                    ? Integer.parseInt(args[2])
                    : 20;
            int linesPerFile = args.length > 3
                    ? Integer.parseInt(args[3])
                    : 1_000;

            GenerateLogs.generate(
                    directory,
                    fileCount,
                    linesPerFile
            );

            System.out.println(
                    "Generated " + fileCount
                            + " files in " + directory
            );
            return;
        }

        if (args.length > 0 && args[0].equals("check")) {
            selfCheck();
            return;
        }

        if (args.length > 0 && args[0].equals("corrupt")) {
            Path dir = Path.of(args.length > 1 ? args[1] : "logs");
            if (!Files.isDirectory(dir)) {
                System.out.println("Папки нет: " + dir
                        + ". Сначала: generate " + dir + " 20 1000");
                return;
            }
            List<LogFile> files = listLogFiles(dir);
            if (files.isEmpty()) {
                System.out.println("Нет .log-файлов в " + dir);
                return;
            }
            Path first = files.get(0).path();
            Files.writeString(first, "ЭТО_НЕ_ЛОГ\n", StandardCharsets.UTF_8,
                    java.nio.file.StandardOpenOption.APPEND);
            System.out.println("Добавили битую строку в " + first);
            return;
        }

        if (args.length > 0 && args[0].equals("analyze")) {
            runAnalyze(args);
            return;
        }

        if (args.length > 0 && args[0].equals("stop-test")) {
            runStopTest(args);
            return;
        }

        System.out.println(
                "Usage: java LogAnalysisAssignment "
                        + "generate <directory> <files> <lines>"
        );
        System.out.println(
                "       java LogAnalysisAssignment check"
        );
    }

    static void runAnalyze(String[] args) throws Exception {
        Path dir = Path.of(args.length > 1 ? args[1] : "logs");
        int pool = args.length > 2 ? Integer.parseInt(args[2]) : 4;
        int sem  = args.length > 3 ? Integer.parseInt(args[3]) : 4;

        if (!Files.isDirectory(dir)) {
            System.out.println("Папки нет: " + dir
                    + ". Сначала: generate " + dir + " 20 1000");
            return;
        }

        List<LogFile> files = listLogFiles(dir);
        if (files.isEmpty()) {
            System.out.println("Нет .log-файлов в " + dir);
            return;
        }
        System.out.println("Файлов: " + files.size()
                + ", pool=" + pool + ", semLimit=" + sem);

        long t0 = System.nanoTime();
        Result seq = analyzeSequential(files);
        long t1 = System.nanoTime();
        printDiagnostics("SEQUENTIAL", (t1 - t0) / 1_000_000L, seq);

        long t2 = System.nanoTime();
        Result par = analyzeParallel(files, pool, sem);
        long t3 = System.nanoTime();
        printDiagnostics("PARALLEL (lock per URL)", (t3 - t2) / 1_000_000L, par);

        long t4 = System.nanoTime();
        Result parGlobal = analyzeGlobal(files, pool, sem);
        long t5 = System.nanoTime();
        printDiagnostics("PARALLEL (global lock)", (t5 - t4) / 1_000_000L, parGlobal);

        System.out.println();
        System.out.println("Совпадение SEQUENTIAL и PARALLEL: "
                + (seq.report.equals(par.report) ? "ДА" : "НЕТ!"));
        System.out.println("Совпадение SEQUENTIAL и GLOBAL: "
                + (seq.report.equals(parGlobal.report) ? "ДА" : "НЕТ!"));

        checkInvariants(par, sem);
    }

    static void runStopTest(String[] args) throws Exception {
        Path dir = Path.of(args.length > 1 ? args[1] : "logs");

        if (!Files.isDirectory(dir)) {
            System.out.println("Папки нет: " + dir
                    + ". Сначала: generate " + dir + " 20 1000");
            return;
        }

        List<LogFile> files = listLogFiles(dir);
        if (files.isEmpty()) {
            System.out.println("Нет .log-файлов в " + dir);
            return;
        }

        stopSignal = false;
        Thread stopper = new Thread(() -> {
            try { Thread.sleep(50); } catch (InterruptedException ignored) {}
            stopSignal = true;
            System.out.println("!!! STOP signal set");
        });
        stopper.start();

        long t0 = System.nanoTime();
        Result r = analyzeParallel(files, 4, 4);
        long t1 = System.nanoTime();
        printDiagnostics("STOP-TEST", (t1 - t0) / 1_000_000L, r);
    }
}