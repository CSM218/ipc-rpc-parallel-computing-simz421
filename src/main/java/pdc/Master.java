package pdc;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * The Master acts as the Coordinator in a distributed cluster.
 * 
 * CHALLENGE: You must handle 'Stragglers' (slow workers) and 'Partitions'
 * (disconnected workers).
 * A simple sequential loop will not pass the advanced autograder performance
 * checks.
 */
public class Master {

    private final ExecutorService systemThreads = Executors.newCachedThreadPool();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ServerSocket serverSocket = null;
    private final Map<String, WorkerInfo> workers = new ConcurrentHashMap<>();
    private static class WorkerInfo {
        public final PrintWriter out;
        public final BufferedReader in;
        public WorkerInfo(Socket s, PrintWriter out, BufferedReader in) {
            this.out = out; this.in = in;
        }
    }

    /**
     * Entry point for a distributed computation.
     * 
     * Students must:
     * 1. Partition the problem into independent 'computational units'.
     * 2. Schedule units across a dynamic pool of workers.
     * 3. Handle result aggregation while maintaining thread safety.
     * 
     * @param operation A string descriptor of the matrix operation (e.g.
     *                  "BLOCK_MULTIPLY")
     * @param data      The raw matrix data to be processed
     */
    public Object coordinate(String operation, int[][] data, int workerCount) {
        // TODO: Architect a scheduling algorithm that survives worker failure.
        // HINT: Think about how MapReduce or Spark handles 'Task Reassignment'.
        return null;
    }

    /**
     * Start the communication listener.
     * Use your custom protocol designed in Message.java.
     */
    public void listen(int port) throws IOException {
        // Start the server socket and spawn a handler per connection.
        systemThreads.submit(() -> {
            try (ServerSocket ss = new ServerSocket(port)) {
                serverSocket = ss;
                running.set(true);
                while (running.get() && !ss.isClosed()) {
                    try {
                        Socket s = ss.accept();
                        systemThreads.submit(() -> handleConnection(s));
                    } catch (IOException e) {
                        if (!running.get()) break;
                    }
                }
            } catch (IOException e) {
                if (serverSocket == null) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private void handleConnection(Socket s) {
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
            PrintWriter out = new PrintWriter(s.getOutputStream(), true);
            String line;
            while ((line = in.readLine()) != null) {
                Message msg = Message.parse(line);
                if (msg == null) continue;
                if ("REGISTER_WORKER".equals(msg.messageType)) {
                    String wid = msg.studentId != null ? msg.studentId : msg.payload;
                    workers.put(wid, new WorkerInfo(s, out, in));
                    // send ack
                    Message ack = new Message("WORKER_ACK", System.getenv("STUDENT_ID"), "");
                    out.println(ack.toJson());
                } else if ("TASK_COMPLETE".equals(msg.messageType)) {
                    // For simplicity, just print receipt; task aggregation is done elsewhere
                    System.out.println("Received TASK_COMPLETE from " + msg.studentId + " payload=" + msg.payload);
                }
            }
        } catch (IOException e) {
            // connection terminated
        }
    }

    /**
     * Distribute a matrix multiplication task across available workers.
     * Splits rows of A across workers and waits for results.
     */
    public int[][] executeMatrixMultiply(int[][] A, int[][] B, long timeoutMillis) throws InterruptedException {
        int n = A.length;
        int m = B[0].length;
        int[][] result = new int[n][m];
        int workerCount = Math.max(1, workers.size());
        String[] wids = workers.keySet().toArray(new String[workers.size()]);
        int parts = Math.max(1, workerCount);
        CountDownLatch latch = new CountDownLatch(parts);
        int rowsPer = (n + parts - 1) / parts;
        for (int p = 0; p < parts; p++) {
            final int start = p * rowsPer;
            final int end = Math.min(n, start + rowsPer);
            if (start >= end) { latch.countDown(); continue; }
            final String wid = wids[p % wids.length];
            final WorkerInfo wi = workers.get(wid);
            systemThreads.submit(() -> {
                try {
                    String payload = encodeAssignment(start, end, A, B);
                    Message req = new Message("RPC_REQUEST", System.getenv("STUDENT_ID"), payload);
                    wi.out.println(req.toJson());
                    // wait for response (blocking read) with timeout
                    long deadline = System.currentTimeMillis() + timeoutMillis;
                    while (System.currentTimeMillis() < deadline) {
                        if (wi.in.ready()) {
                            String line = wi.in.readLine();
                            Message resp = Message.parse(line);
                            if (resp != null && ("TASK_COMPLETE".equals(resp.messageType))) {
                                applyResult(result, resp.payload);
                                break;
                            }
                        } else {
                            Thread.sleep(10);
                        }
                    }
                } catch (IOException | InterruptedException e) {
                    // on failure, caller could reassign; for now we log
                    System.err.println("Worker task failure: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await(timeoutMillis + 1000, TimeUnit.MILLISECONDS);
        return result;
    }

    private static String encodeAssignment(int start, int end, int[][] A, int[][] B) {
        StringBuilder sb = new StringBuilder();
        sb.append(start).append('|').append(end).append('|');
        sb.append(serializeMatrixRange(A, start, end));
        sb.append('|');
        sb.append(serializeMatrix(B));
        return sb.toString();
    }

    private static String serializeMatrixRange(int[][] M, int start, int end) {
        StringBuilder sb = new StringBuilder();
        for (int i = start; i < end; i++) {
            for (int j = 0; j < M[i].length; j++) {
                if (j > 0) sb.append(',');
                sb.append(M[i][j]);
            }
            if (i < end - 1) sb.append(";");
        }
        return sb.toString();
    }

    private static String serializeMatrix(int[][] M) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < M.length; i++) {
            for (int j = 0; j < M[i].length; j++) {
                if (j > 0) sb.append(',');
                sb.append(M[i][j]);
            }
            if (i < M.length - 1) sb.append(";");
        }
        return sb.toString();
    }

    private static void applyResult(int[][] result, String payload) {
        // payload format: start|end|rows
        try {
            String[] parts = payload.split("\\|",3);
            int start = Integer.parseInt(parts[0]);
            // int end = Integer.parseInt(parts[1]);
            String rows = parts[2];
            String[] rowArr = rows.split(";");
            for (int i = 0; i < rowArr.length; i++) {
                String[] vals = rowArr[i].split(",");
                for (int j = 0; j < vals.length; j++) {
                    result[start + i][j] = Integer.parseInt(vals[j]);
                }
            }
        } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
            System.err.println("Failed to apply result: " + e.getMessage());
        }
    }

    /**
     * System Health Check.
     * Detects dead workers and re-integrates recovered workers.
     */
    public void reconcileState() {
        // No-op maintenance hook for tests; real reconciliation should
        // inspect worker liveness and reassign tasks.
    }

    public static void main(String[] args) throws Exception {
        int port = 9999;
        String p = System.getenv("MASTER_PORT");
        if (p != null) try { port = Integer.parseInt(p); } catch (NumberFormatException ignored) {}
        Master master = new Master();
        master.listen(port);
        System.out.println("Master listening on port " + port);
    }
}
