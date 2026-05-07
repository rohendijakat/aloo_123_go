/*
 * aloo_123_go — single-file Java app
 *
 * Local “AI trading bot” workbench:
 * - HTTP API (no external dependencies)
 * - Strategy registry (EMA + RSI rule set)
 * - CSV candle parsing + backtest simulator
 * - Paper broker with fees/slippage + equity curve
 * - Server-Sent Events (SSE) log stream for the Inf web UI
 *
 * Build (JDK 17+):
 *   javac Aloo123Go.java
 *   java Aloo123Go --port 8787
 */

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class Aloo123Go {
    public static void main(String[] args) throws Exception {
        Cli cli = Cli.parse(args);
        LogBus logs = new LogBus(3000);
        Store store = new Store(cli.dataDir, logs);
        Engine engine = new Engine(store, logs);
        Api api = new Api(cli, store, engine, logs);
        api.start();
        logs.info("boot", "aloo_123_go on http://" + cli.bind + ":" + cli.port);
        logs.info("boot", "Inf UI: set API base to http://localhost:" + cli.port);
    }

    // -------------------------------- CLI --------------------------------
    static final class Cli {
        final int port;
        final String bind;
        final Path dataDir;
        final boolean cors;

        Cli(int port, String bind, Path dataDir, boolean cors) {
            this.port = port;
            this.bind = bind;
            this.dataDir = dataDir;
            this.cors = cors;
        }

        static Cli parse(String[] args) {
            int port = 8787;
            String bind = "127.0.0.1";
            Path data = Path.of(".", ".aloo_123_go");
            boolean cors = true;
            for (int i = 0; i < args.length; i++) {
                String a = args[i];
                if (a.equals("--port") && i + 1 < args.length) port = Integer.parseInt(args[++i]);
                else if (a.equals("--bind") && i + 1 < args.length) bind = args[++i];
                else if (a.equals("--data") && i + 1 < args.length) data = Path.of(args[++i]);
                else if (a.equals("--cors")) cors = true;
                else if (a.equals("--no-cors")) cors = false;
            }
            return new Cli(port, bind, data, cors);
        }
    }

    // -------------------------------- API --------------------------------
    interface Handler { void handle(HttpExchange ex) throws Exception; }

