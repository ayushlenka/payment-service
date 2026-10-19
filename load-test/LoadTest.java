import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Minimal load driver for POST /payments. Run with Java 21 single-file launch:
 *   java -Dtotal=2000 -Dconcurrency=64 -Daccounts=<uuid,uuid,...> LoadTest.java
 * Reports submit throughput and POST latency percentiles. End-to-end completion
 * latency and balance drift are measured separately from the database.
 */
public class LoadTest {

    public static void main(String[] args) throws Exception {
        int total = Integer.getInteger("total", 2000);
        int concurrency = Integer.getInteger("concurrency", 64);
        String base = System.getProperty("base", "http://localhost:8080");
        String[] accounts = System.getProperty("accounts").split(",");

        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        Semaphore inFlight = new Semaphore(concurrency);
        long[] latencyMs = new long[total];
        AtomicInteger created = new AtomicInteger();
        AtomicInteger other = new AtomicInteger();
        Random random = new Random();

        long start = System.nanoTime();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < total; i++) {
                final int idx = i;
                inFlight.acquire();
                futures.add(executor.submit(() -> {
                    try {
                        int a = random.nextInt(accounts.length);
                        int b;
                        do {
                            b = random.nextInt(accounts.length);
                        } while (b == a);
                        long amount = 1 + random.nextInt(1000);
                        String body = "{\"fromAccountId\":\"" + accounts[a] + "\",\"toAccountId\":\""
                                + accounts[b] + "\",\"amountMinor\":" + amount + ",\"currency\":\"USD\"}";
                        HttpRequest request = HttpRequest.newBuilder(URI.create(base + "/payments"))
                                .header("Idempotency-Key", "load-" + idx + "-" + UUID.randomUUID())
                                .header("Content-Type", "application/json")
                                .POST(HttpRequest.BodyPublishers.ofString(body))
                                .build();
                        long t0 = System.nanoTime();
                        HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());
                        latencyMs[idx] = (System.nanoTime() - t0) / 1_000_000;
                        if (response.statusCode() == 201) {
                            created.incrementAndGet();
                        } else {
                            other.incrementAndGet();
                        }
                    } catch (Exception e) {
                        other.incrementAndGet();
                        latencyMs[idx] = -1;
                    } finally {
                        inFlight.release();
                    }
                }));
            }
            for (Future<?> f : futures) {
                f.get();
            }
        }
        double seconds = (System.nanoTime() - start) / 1e9;

        long[] sorted = Arrays.stream(latencyMs).filter(l -> l >= 0).sorted().toArray();
        System.out.printf("submitted=%d created(201)=%d other=%d%n", total, created.get(), other.get());
        System.out.printf("submit throughput = %.0f req/s over %.2fs%n", total / seconds, seconds);
        if (sorted.length > 0) {
            System.out.printf("POST latency ms: p50=%d p90=%d p99=%d max=%d%n",
                    percentile(sorted, 50), percentile(sorted, 90), percentile(sorted, 99),
                    sorted[sorted.length - 1]);
        }
    }

    private static long percentile(long[] sorted, int p) {
        return sorted[(int) Math.ceil(p / 100.0 * sorted.length) - 1];
    }
}
