package dev.harsharocks.orders;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;

/**
 * Single-process durable inbox for normalized order events.
 *
 * Records are checksummed and synced before the in-memory projection is updated.
 * Reopening the journal replays all recorded events, including stale events and any
 * duplicate records written by older versions,
 * so previously observed event IDs retain their deduplication semantics after restart.
 * An exclusive OS file lock prevents two local processes from writing concurrently.
 * This is a teaching example, not a replacement for a transactional database.
 */
public final class OrderEventJournal implements AutoCloseable {
    private static final int MAX_RECORD_BYTES = 16 * 1024;
    private static final String FORMAT_VERSION = "v1";

    private final Path path;
    private final FileChannel channel;
    private final FileLock lock;
    private final OrderEventProcessor processor = new OrderEventProcessor();
    private boolean closed;
    private boolean failed;

    public OrderEventJournal(Path path) throws IOException {
        this.path = Objects.requireNonNull(path, "path must not be null").toAbsolutePath();
        Path parent = this.path.getParent();
        if (parent != null) Files.createDirectories(parent);

        FileChannel opened = FileChannel.open(this.path,
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        FileLock acquired = null;
        try {
            try {
                acquired = opened.tryLock();
            } catch (OverlappingFileLockException e) {
                throw new IOException("Journal is already open: " + this.path, e);
            }
            if (acquired == null) throw new IOException("Journal is locked: " + this.path);
            replay(opened);
            opened.position(opened.size());
        } catch (IOException | RuntimeException e) {
            try {
                if (acquired != null) acquired.release();
            } finally {
                opened.close();
            }
            throw e;
        }
        this.channel = opened;
        this.lock = acquired;
    }

    /**
     * Persists previously unseen event IDs before applying them. Exact retries and
     * conflicting reuse of an event ID do not change state and need no new disk record.
     * Rejects writes after an I/O failure.
     */
    public synchronized ProcessingResult accept(OrderEvent event) throws IOException {
        ensureUsable();
        Objects.requireNonNull(event, "event must not be null");
        Optional<ProcessingResult> duplicate = processor.alreadySeen(event);
        if (duplicate.isPresent()) return duplicate.get();
        byte[] bytes = serialize(event);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        try {
            while (buffer.hasRemaining()) channel.write(buffer);
            channel.force(true);
        } catch (IOException | RuntimeException e) {
            failed = true; // A partial append must not be followed by more writes.
            throw e;
        }
        return processor.accept(event);
    }

    public synchronized Optional<OrderSnapshot> get(String restaurantId, String orderId) {
        ensureUsable();
        return processor.get(restaurantId, orderId);
    }

    public synchronized int observedEvents() {
        ensureUsable();
        return processor.observedEvents();
    }

    private void ensureUsable() {
        if (closed) throw new IllegalStateException("Journal is closed");
        if (failed) throw new IllegalStateException("Journal write failed; reopen after recovery");
    }

    private void replay(FileChannel source) throws IOException {
        source.position(0);
        ByteBuffer chunk = ByteBuffer.allocate(8192);
        ByteArrayOutputStream record = new ByteArrayOutputStream();
        long lineNumber = 1;
        while (source.read(chunk) != -1) {
            chunk.flip();
            while (chunk.hasRemaining()) {
                int next = Byte.toUnsignedInt(chunk.get());
                if (next == '\n') {
                    try {
                        processor.accept(deserialize(record.toByteArray()));
                    } catch (RuntimeException e) {
                        throw new IOException("Invalid journal record at line " + lineNumber, e);
                    }
                    record.reset();
                    lineNumber++;
                } else {
                    if (record.size() >= MAX_RECORD_BYTES) {
                        throw new IOException("Journal record too large at line " + lineNumber);
                    }
                    record.write(next);
                }
            }
            chunk.clear();
        }
        if (record.size() != 0) {
            throw new IOException("Incomplete journal record at line " + lineNumber);
        }
    }

    private static byte[] serialize(OrderEvent event) {
        String payload = String.join("\t", FORMAT_VERSION,
                encode(event.eventId()), encode(event.restaurantId()),
                encode(event.orderId()), encode(event.occurredAt().toString()),
                encode(event.status()), Long.toString(event.totalCents()));
        String record = payload + "\t" + checksum(payload) + "\n";
        byte[] bytes = record.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_RECORD_BYTES) {
            throw new IllegalArgumentException("Event is too large for the journal");
        }
        return bytes;
    }

    private static OrderEvent deserialize(byte[] bytes) {
        String[] parts = new String(bytes, StandardCharsets.UTF_8).split("\t", -1);
        if (parts.length != 8 || !FORMAT_VERSION.equals(parts[0])) {
            throw new IllegalArgumentException("Invalid journal format");
        }
        String payload = String.join("\t", java.util.Arrays.copyOf(parts, 7));
        if (!checksum(payload).equals(parts[7])) {
            throw new IllegalArgumentException("Journal checksum mismatch");
        }
        return new OrderEvent(decode(parts[1]), decode(parts[2]), decode(parts[3]),
                Instant.parse(decode(parts[4])), decode(parts[5]), Long.parseLong(parts[6]));
    }

    private static String encode(String value) {
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String decode(String value) {
        return new String(Base64.getUrlDecoder().decode(value), StandardCharsets.UTF_8);
    }

    private static String checksum(String payload) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(payload.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) return;
        closed = true;
        try {
            lock.release();
        } finally {
            channel.close();
        }
    }
}
