import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.sql.*;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

public class MultiThreadedEmailCrawler {

    private static final String SEED_URL   = "https://www.touro.edu";
    private static final String DOMAIN     = "touro.edu";
    private static final int    MAX_EMAILS = 10_000;
    private static final int    BATCH_SIZE = 500;
    private static final int    N_THREADS  = Runtime.getRuntime().availableProcessors();

    private final String DB_URL;
    private final String DB_USER;
    private final String DB_PASSWORD;

    private final BlockingQueue<String> urlQueue    = new LinkedBlockingQueue<>();
    private final Set<String>           visited     = ConcurrentHashMap.newKeySet();
    private final Set<String>           foundEmails = ConcurrentHashMap.newKeySet();
    private final BlockingQueue<EmailRecord> emailQueue = new LinkedBlockingQueue<>();
    private final AtomicInteger emailCount   = new AtomicInteger(0);
    private final AtomicBoolean stopFlag     = new AtomicBoolean(false);

    private static final Pattern EMAIL_PATTERN =
            Pattern.compile("[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[A-Za-z]{2,}");

    public static void main(String[] args) throws Exception {
        new MultiThreadedEmailCrawler().start();
    }

    public MultiThreadedEmailCrawler() throws IOException, ClassNotFoundException, SQLException {

        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream("db.properties")) {
            props.load(in);
        }
        DB_URL      = props.getProperty("db.url");
        DB_USER     = props.getProperty("db.user");
        DB_PASSWORD = props.getProperty("db.password");

        try (Connection test = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {
            System.out.println("Successfully connected to database.");
        }

        urlQueue.add(SEED_URL);
    }

    public void start() throws InterruptedException {
        System.out.printf("Starting crawl (%d threads) at %s%n", N_THREADS, SEED_URL);

        Thread dbWriter = new Thread(this::dbWriterLoop, "DB-Writer");
        dbWriter.start();

        ExecutorService pool = Executors.newFixedThreadPool(N_THREADS);
        IntStream.range(0, N_THREADS).forEach(i -> pool.submit(this::crawlWorker));

        pool.shutdown();
        while (!pool.awaitTermination(1, TimeUnit.SECONDS)) {
            if (emailCount.get() >= MAX_EMAILS) {
                stopFlag.set(true);
                pool.shutdownNow();
            }
        }

        stopFlag.set(true);
        dbWriter.join();

        System.out.println("Crawl finished. Total unique emails: " + emailCount.get());
    }

    private void crawlWorker() {
        try {
            while (!stopFlag.get()) {
                String url = urlQueue.poll(2, TimeUnit.SECONDS);
                if (url == null) {
                    if (emailCount.get() >= MAX_EMAILS) break;
                    else continue;
                }
                if (!visited.add(url)) continue;
                processPage(url);
            }
        } catch (InterruptedException ignored) { }
    }

    private void processPage(String url) {
        Document doc;
        try {
            doc = Jsoup.connect(url).get();
        } catch (IOException e) {
            System.err.println("Failed to fetch " + url + ": " + e.getMessage());
            return;
        }
        extractEmails(url, doc.text());
        enqueueLinks(doc.select("a[href]"));
    }

    private void extractEmails(String sourceUrl, String text) {
        Matcher m = EMAIL_PATTERN.matcher(text);
        while (m.find() && emailCount.get() < MAX_EMAILS) {
            String email = m.group().toLowerCase();
            if (foundEmails.add(email)) {
                int count = emailCount.incrementAndGet();
                emailQueue.offer(new EmailRecord(email, sourceUrl,
                        new Timestamp(System.currentTimeMillis())));
                System.out.printf("Found email #%d: %s (from %s)%n",
                        count, email, sourceUrl);
                if (count >= MAX_EMAILS) {
                    stopFlag.set(true);
                    break;
                }
            }
        }
    }

    private void enqueueLinks(Elements links) {
        for (Element link : links) {
            String next = link.absUrl("href");
            if (next.isEmpty()) continue;
            if (isSameDomain(next) && !visited.contains(next)) {
                urlQueue.offer(next);
            }
        }
    }

    private boolean isSameDomain(String urlStr) {
        try {
            String host = new URL(urlStr).getHost().toLowerCase();
            return host.endsWith(DOMAIN);
        } catch (Exception e) {
            return false;
        }
    }

    private void dbWriterLoop() {
        List<EmailRecord> batch = new ArrayList<>(BATCH_SIZE);
        while (!stopFlag.get() || !emailQueue.isEmpty()) {
            try {
                EmailRecord rec = emailQueue.poll(2, TimeUnit.SECONDS);
                if (rec != null) batch.add(rec);

                if (batch.size() >= BATCH_SIZE || (stopFlag.get() && !batch.isEmpty())) {
                    bulkInsert(batch);
                    batch.clear();
                }
            } catch (InterruptedException ignored) { }
        }
        if (!batch.isEmpty()) {
            bulkInsert(batch);
            batch.clear();
        }
    }

    private void bulkInsert(List<EmailRecord> batch) {
        String sql = "INSERT INTO Emails (EmailAddress, Source, TimeStamp) VALUES (?, ?, ?)";
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             PreparedStatement ps = conn.prepareStatement(sql)) {

            conn.setAutoCommit(false);
            for (EmailRecord r : batch) {
                ps.setString (1, r.address);
                ps.setString (2, r.sourceUrl);
                ps.setTimestamp(3, r.foundAt);
                ps.addBatch();
            }
            int[] results = ps.executeBatch();
            conn.commit();

            int inserted = Arrays.stream(results).sum();
            System.out.printf("   ➤ Flushed %4d records to DB (reported %d inserts, total %4d)%n",
                    batch.size(), inserted, emailCount.get());
        } catch (SQLException e) {
            System.err.println("DB insert error (batch size " + batch.size() + "):");
            e.printStackTrace();
        }
    }

    private static class EmailRecord {
        final String    address;
        final String    sourceUrl;
        final Timestamp foundAt;
        EmailRecord(String a, String s, Timestamp t) {
            address   = a;
            sourceUrl = s;
            foundAt   = t;
        }
    }
}

/*
Data Structures:
- A FIFO queue of URLs to visit: Queue<String> urlQueue
- A set of already‐visited URLs to avoid repeats: Set<String> visitedUrls
- A set of unique email addresses found: Set<String> foundEmails
- An in-memory buffer of EmailRecord objects for bulk insertion: List<EmailRecord> buffer

Pseudocode (single-threaded):

1. Add the seed URL (https://www.touro.edu) to urlQueue and leave visitedUrls, foundEmails, and buffer empty.
2. While urlQueue is not empty and foundEmails.size() is less than the target:
     a. Dequeue the next URL from urlQueue into currentUrl.
     b. If currentUrl is already in visitedUrls, skip to the next iteration.
     c. Otherwise, add currentUrl to visitedUrls.
     d. Fetch the page at currentUrl using Jsoup and parse its text content.
     e. Use a precompiled email‐matching regex to scan the page text; for each match:
          i.  Normalize the email to lowercase.
         ii.  If foundEmails does not already contain it, add it to foundEmails.
        iii.  Create an EmailRecord(email, currentUrl, currentTimestamp) and add it to buffer.
         iv.  If buffer.size() reaches the batch threshold (e.g. 500), perform a bulk insert of buffer into the database and then clear buffer.
          v.  If foundEmails.size() has reached the maximum, break out of both loops.
     f. Select all anchor tags (<a href>) from the document; for each link:
          i.   Convert to an absolute URL.
         ii.   If the URL’s host ends with “touro.edu” and it is not in visitedUrls, enqueue it onto urlQueue.
3. After the loop ends, if buffer is not empty, perform one final bulk insert of the remaining EmailRecords.
4. Log the total count of emails discovered and terminate.

This approach ensures breadth-first traversal on a single thread, avoids revisiting pages, buffers writes for efficiency, and stops when the target number of unique emails is reached.
*/