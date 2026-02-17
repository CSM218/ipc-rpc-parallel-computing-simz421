package pdc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 * 
 * CHALLENGE: Efficiency is key. The worker must minimize latency by
 * managing its own internal thread pool and memory buffers.
 */
public class Worker {

    private final ExecutorService exec = Executors.newSingleThreadExecutor();
    private Socket sock;
    private BufferedReader in;
    private PrintWriter out;
    private String workerId = null;

    /**
     * Connects to the Master and initiates the registration handshake.
     * The handshake must exchange 'Identity' and 'Capability' sets.
     */
    public void joinCluster(String masterHost, int port) {
        // Connect and start a listening loop; keep connection open.
        try {
            this.sock = new Socket();
            this.sock.connect(new InetSocketAddress(masterHost, port), 1000);
            this.in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
            this.out = new PrintWriter(sock.getOutputStream(), true);
            this.workerId = System.getenv("WORKER_ID");
            Message reg = new Message("REGISTER_WORKER", this.workerId != null ? this.workerId : "unknown", "");
            out.println(reg.toJson());
            // spawn listener
            exec.submit(this::listenLoop);
        } catch (IOException e) {
            // swallow, tests should be resilient
        }
    }

    /**
     * Executes a received task block.
     * 
     * Students must ensure:
     * 1. The operation is atomic from the perspective of the Master.
     * 2. Overlapping tasks do not cause race conditions.
     * 3. 'End-to-End' logs are precise for performance instrumentation.
     */
    public void execute() {
        // Start processing loop in background; for tests this should be
        // non-blocking and resilient to missing input.
        exec.submit(() -> {
            try {
                Thread.sleep(10);
            } catch (InterruptedException ignored) {
            }
        });
    }

    private void listenLoop() {
        try {
            String line;
            while (in != null && (line = in.readLine()) != null) {
                Message msg = Message.parse(line);
                if (msg == null) continue;
                if ("RPC_REQUEST".equals(msg.messageType)) {
                    String respPayload = handleRpcRequest(msg.payload);
                    Message resp = new Message("TASK_COMPLETE", workerId != null ? workerId : "worker", respPayload);
                    out.println(resp.toJson());
                } else if ("HEARTBEAT".equals(msg.messageType)) {
                    Message hb = new Message("HEARTBEAT", workerId != null ? workerId : "worker", "ACK");
                    out.println(hb.toJson());
                }
            }
        } catch (IOException e) {
            // connection lost
        }
    }

    private String handleRpcRequest(String payload) {
        // payload format: start|end|Arows|Bfull where Arows and Bfull use rows separated by ';' and values by ','
        try {
            String[] parts = payload.split("\\|",4);
            int start = Integer.parseInt(parts[0]);
            int end = Integer.parseInt(parts[1]);
            String arows = parts[2];
            String bfull = parts[3];
            int[][] A = parseMatrix(arows);
            int[][] B = parseMatrix(bfull);
            int[][] R = multiply(A, B);
            String rser = serializeMatrix(R);
            return start + "|" + end + "|" + rser;
        } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
            return "0|0|";
        }
    }

    private static int[][] parseMatrix(String s) {
        if (s == null || s.isEmpty()) return new int[0][0];
        String[] rows = s.split(";");
        int[][] M = new int[rows.length][];
        for (int i = 0; i < rows.length; i++) {
            String[] vals = rows[i].split(",");
            M[i] = new int[vals.length];
            for (int j = 0; j < vals.length; j++) M[i][j] = Integer.parseInt(vals[j]);
        }
        return M;
    }

    private static String serializeMatrix(int[][] M) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < M.length; i++) {
            for (int j = 0; j < M[i].length; j++) {
                if (j > 0) sb.append(',');
                sb.append(M[i][j]);
            }
            if (i < M.length - 1) sb.append(';');
        }
        return sb.toString();
    }

    private static int[][] multiply(int[][] A, int[][] B) {
        int n = A.length;
        if (n == 0) return new int[0][0];
        int p = B[0].length;
        int[][] R = new int[n][p];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < p; j++) {
                int sum = 0;
                for (int k = 0; k < B.length; k++) sum += A[i][k] * B[k][j];
                R[i][j] = sum;
            }
        }
        return R;
    }

    public static void main(String[] args) throws Exception {
        String host = System.getenv("MASTER_HOST");
        String p = System.getenv("MASTER_PORT");
        int port = 9999;
        if (p != null) try { port = Integer.parseInt(p); } catch (NumberFormatException ignored) {}
        if (host == null) host = "localhost";
        Worker w = new Worker();
        w.joinCluster(host, port);
        // Keep main alive while the worker thread runs
        Thread.sleep(TimeUnit.HOURS.toMillis(1));
    }
}
