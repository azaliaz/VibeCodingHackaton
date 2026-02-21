import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * TestRunner: runs a command on all *.in tests and compares stdout with *.out.
 * <p>
 * Usage:
 * javac TestRunner.java
 * <p>
 * # Run oracle on all tests:
 * java TestRunner ./tests -- java -cp . Oracle r4
 * <p>
 * # Run a team's solution:
 * java TestRunner ./tests -- java -cp ./teams/teamA Solution
 * <p>
 * Comparison:
 * - Normalize CRLF/CR to LF
 * - Strip trailing spaces/tabs on each line
 * - Ignore extra empty lines at end
 * - Ensure exactly one trailing newline for stable comparison
 * <p>
 * Diff:
 * - On WA prints first differing line number + expected/got lines (escaped)
 * - Also reports if one output ended earlier
 */
public class TestRunner {

    record TestCase(Path inFile, Path outFile, String name) {
    }

    record DiffInfo(
            boolean equal,
            int lineNo,              // 1-based; -1 if equal
            String expectedLine,
            String gotLine,
            String note              // optional (e.g., "got ended early")
    ) {
    }

    record RunResult(
            String name,
            boolean passed,
            int exitCode,
            long millis,
            String stdout,
            String stderr,
            String failureReason,
            DiffInfo diff
    ) {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            printUsageAndExit();
            return;
        }

        int sep = indexOf(args, "--");
        if (sep < 0 || sep == args.length - 1) {
            printUsageAndExit();
            return;
        }

        Path testsDir = Paths.get(args[0]).toAbsolutePath().normalize();
        List<String> command = Arrays.asList(Arrays.copyOfRange(args, sep + 1, args.length));

        if (!Files.isDirectory(testsDir)) {
            System.err.println("ERROR: tests dir does not exist or not a directory: " + testsDir);
            System.exit(2);
        }
        if (command.isEmpty()) {
            System.err.println("ERROR: empty command");
            System.exit(2);
        }

        List<TestCase> tests = discoverTests(testsDir);
        if (tests.isEmpty()) {
            System.err.println("No tests found in: " + testsDir + " (expected *.in files)");
            System.exit(2);
        }

        System.out.println("== TestRunner ==");
        System.out.println("Tests dir: " + testsDir);
        System.out.println("Command : " + String.join(" ", command));
        System.out.println("Found   : " + tests.size() + " test(s)");
        System.out.println();

        List<RunResult> results = new ArrayList<>(tests.size());
        for (TestCase tc : tests) {
            RunResult rr = runOne(tc, command);
            results.add(rr);
            printOne(rr);
        }

        System.out.println();
        printSummary(results);
    }

    // -------------------- Discover tests --------------------
    static List<TestCase> discoverTests(Path testsDir) throws IOException {
        try (var stream = Files.list(testsDir)) {
            List<Path> ins = stream
                    .filter(p -> p.getFileName().toString().endsWith(".in"))
                    .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                    .toList();

            List<TestCase> tests = new ArrayList<>();
            for (Path in : ins) {
                String file = in.getFileName().toString();
                String base = file.substring(0, file.length() - 3); // remove ".in"
                Path out = testsDir.resolve(base + ".out");
                if (!Files.isRegularFile(out)) {
                    throw new FileNotFoundException("Missing expected output file for " + file + ": " + out);
                }
                tests.add(new TestCase(in, out, base));
            }
            return tests;
        }
    }

    // -------------------- Run one test --------------------
    static RunResult runOne(TestCase tc, List<String> command) {
        Instant start = Instant.now();

        String stdout = "";
        String stderr = "";
        int exit = -999;

        try {
            ProcessBuilder pb = new ProcessBuilder(command);
            pb.redirectInput(tc.inFile.toFile());
            Process p = pb.start();

            ByteArrayOutputStream outBuf = new ByteArrayOutputStream();
            ByteArrayOutputStream errBuf = new ByteArrayOutputStream();

            Thread tOut = new Thread(() -> copyStream(p.getInputStream(), outBuf), "stdout-reader");
            Thread tErr = new Thread(() -> copyStream(p.getErrorStream(), errBuf), "stderr-reader");
            tOut.start();
            tErr.start();

            exit = p.waitFor();
            tOut.join();
            tErr.join();

            stdout = outBuf.toString(StandardCharsets.UTF_8);
            stderr = errBuf.toString(StandardCharsets.UTF_8);

        } catch (Exception e) {
            long ms = Duration.between(start, Instant.now()).toMillis();
            return new RunResult(tc.name, false, exit, ms, stdout, stderr, "Runner exception: " + e, null);
        }

        long ms = Duration.between(start, Instant.now()).toMillis();

        String expected;
        try {
            expected = Files.readString(tc.outFile, StandardCharsets.UTF_8);
        } catch (IOException e) {
            return new RunResult(tc.name, false, exit, ms, stdout, stderr, "Cannot read expected output: " + e, null);
        }

        if (exit != 0) {
            return new RunResult(tc.name, false, exit, ms, stdout, stderr, "Non-zero exit code", null);
        }

        String gotN = normalize(stdout);
        String expN = normalize(expected);

        DiffInfo diff = firstDiff(expN, gotN);
        boolean ok = diff.equal;

        if (!ok) {
            return new RunResult(tc.name, false, exit, ms, stdout, stderr, "Wrong answer", diff);
        }
        return new RunResult(tc.name, true, exit, ms, stdout, stderr, null, diff);
    }

    // -------------------- Diff --------------------
    static DiffInfo firstDiff(String expectedNormalized, String gotNormalized) {
        // Both inputs expected to end with exactly one '\n'
        String[] exp = expectedNormalized.split("\n", -1);
        String[] got = gotNormalized.split("\n", -1);

        // Because normalize() ensures a trailing newline, the last element is "" in both arrays.
        int max = Math.max(exp.length, got.length);
        for (int i = 0; i < max; i++) {
            String e = (i < exp.length) ? exp[i] : null;
            String g = (i < got.length) ? got[i] : null;

            if (Objects.equals(e, g)) continue;

            int lineNo = i + 1;
            String note = null;
            if (e == null) note = "expected ended early (got has extra lines)";
            else if (g == null) note = "got ended early (expected has more lines)";

            return new DiffInfo(false, lineNo, e, g, note);
        }
        return new DiffInfo(true, -1, null, null, null);
    }

    // -------------------- Pretty output --------------------
    static void printOne(RunResult rr) {
        String status = rr.passed ? "PASS" : "FAIL";
        String time = String.format("%4d ms", rr.millis);
        String extra = rr.passed ? "" : ("  (" + rr.failureReason + ", exit=" + rr.exitCode + ")");
        System.out.println(status + "  " + time + "  " + rr.name + extra);

        if (!rr.passed) {
            // stderr preview
            String err = preview(rr.stderr, 300);
            if (!err.isEmpty()) {
                System.out.println("  stderr: " + escapeOneLine(err));
            }

            if (rr.diff != null && !rr.diff.equal) {
                System.out.println("  diff  : first mismatch at line " + rr.diff.lineNo +
                        (rr.diff.note != null ? " [" + rr.diff.note + "]" : ""));
                System.out.println("  exp   : " + escapeForDiff(rr.diff.expectedLine));
                System.out.println("  got   : " + escapeForDiff(rr.diff.gotLine));
            } else {
                // fallback preview
                String got = preview(normalize(rr.stdout), 400);
                System.out.println("  stdout: " + (got.isEmpty() ? "<empty>" : escapeOneLine(got)));
            }
        }
    }

    static void printSummary(List<RunResult> results) {
        long passed = results.stream().filter(r -> r.passed).count();
        long failed = results.size() - passed;

        System.out.println("== Summary ==");
        System.out.println("Total : " + results.size());
        System.out.println("Passed: " + passed);
        System.out.println("Failed: " + failed);

        if (passed > 0) {
            System.out.println();
            System.out.println("Passed tests:");
            System.out.println(results.stream()
                    .filter(r -> r.passed)
                    .map(r -> "  - " + r.name + " (" + r.millis + " ms)")
                    .collect(Collectors.joining("\n")));
        }

        if (failed > 0) {
            System.out.println();
            System.out.println("Failed tests:");
            System.out.println(results.stream()
                    .filter(r -> !r.passed)
                    .map(r -> "  - " + r.name + " (" + r.failureReason + ", exit=" + r.exitCode + ")")
                    .collect(Collectors.joining("\n")));
        }
    }

    // -------------------- Helpers --------------------
    static int indexOf(String[] arr, String s) {
        for (int i = 0; i < arr.length; i++) if (Objects.equals(arr[i], s)) return i;
        return -1;
    }

    static void copyStream(InputStream in, OutputStream out) {
        try (in; out) {
            byte[] buf = new byte[8192];
            int n;
            while ((n = in.read(buf)) >= 0) {
                if (n == 0) continue;
                out.write(buf, 0, n);
            }
            out.flush();
        } catch (IOException ignored) {
        }
    }

    /**
     * Normalization used for comparison:
     * - normalize line endings to '\n'
     * - strip trailing whitespace at end of each line
     * - ignore extra empty lines at end
     * - ensure exactly one trailing newline at end
     */
    static String normalize(String s) {
        String x = s.replace("\r\n", "\n").replace("\r", "\n");
        String[] lines = x.split("\n", -1);

        for (int i = 0; i < lines.length; i++) {
            lines[i] = rstrip(lines[i]);
        }

        int end = lines.length;
        while (end > 0 && lines[end - 1].isEmpty()) end--;

        String joined = String.join("\n", Arrays.copyOf(lines, end));
        return joined + "\n";
    }

    static String rstrip(String s) {
        int i = s.length() - 1;
        while (i >= 0) {
            char c = s.charAt(i);
            if (c == ' ' || c == '\t') i--;
            else break;
        }
        return s.substring(0, i + 1);
    }

    static String preview(String s, int maxChars) {
        if (s == null) return "";
        if (s.length() <= maxChars) return s;
        return s.substring(0, maxChars) + "...";
    }

    static String escapeOneLine(String s) {
        return s.replace("\\", "\\\\")
                .replace("\n", "\\n")
                .replace("\t", "\\t")
                .replace("\r", "\\r");
    }

    static String escapeForDiff(String s) {
        if (s == null) return "<null>";
        if (s.isEmpty()) return "<empty line>";
        return escapeOneLine(s);
    }

    static void printUsageAndExit() {
        System.err.println("""
                Usage:
                  java TestRunner <tests_dir> -- <command...>
                
                Examples:
                  java TestRunner ./tests -- java -cp . Oracle r4
                  java TestRunner ./tests -- java -cp ./teams/teamA Solution
                """);
        System.exit(2);
    }
}