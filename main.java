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

    static final class Api implements Closeable {
        private final Cli cli;
        private final Store store;
        private final Engine engine;
        private final LogBus logs;
        private final HttpServer server;
        private final ExecutorService pool;
        private final ScheduledExecutorService scheduler;

        Api(Cli cli, Store store, Engine engine, LogBus logs) throws IOException {
            this.cli = cli;
            this.store = store;
            this.engine = engine;
            this.logs = logs;
            this.server = HttpServer.create(new InetSocketAddress(cli.bind, cli.port), 0);
            this.pool = Executors.newFixedThreadPool(Math.max(4, Runtime.getRuntime().availableProcessors()));
            this.scheduler = Executors.newSingleThreadScheduledExecutor();
            server.setExecutor(pool);
            routes();
        }

        void start() {
            scheduler.scheduleAtFixedRate(() -> {
                try { store.flushIfDirty(); }
                catch (Exception e) { logs.error("persist", "flush failed: " + e.getMessage()); }
            }, 2, 2, TimeUnit.SECONDS);
            server.start();
        }

        @Override public void close() {
            try { scheduler.shutdownNow(); } catch (Exception ignored) {}
            try { pool.shutdownNow(); } catch (Exception ignored) {}
            try { server.stop(0); } catch (Exception ignored) {}
        }

        private void routes() {
            add("/api/status", ex -> respondJson(ex, 200,
                    new Json.Obj()
                            .put("ok", true)
                            .put("name", "aloo_123_go")
                            .put("ts", System.currentTimeMillis())
                            .put("dataDir", cli.dataDir.toAbsolutePath().toString())
            ));

            add("/api/strategies", ex -> {
                if (method(ex, "GET")) {
                    Json.Arr arr = new Json.Arr();
                    for (StrategyDef s : store.listStrategies()) arr.add(s.toJson());
                    respondJson(ex, 200, new Json.Obj().put("ok", true).put("strategies", arr));
                } else if (method(ex, "POST")) {
                    Json.Obj body = readJsonObj(ex);
                    StrategyDef s = StrategyDef.fromJson(body, StrategyDef.defaults());
                    store.upsertStrategy(s);
                    logs.info("strategy", "upsert " + s.id + " (" + s.name + ")");
                    respondJson(ex, 200, new Json.Obj().put("ok", true).put("strategy", s.toJson()));
                } else if (method(ex, "DELETE")) {
                    String id = q(ex.getRequestURI(), "id").orElse(null);
                    if (id == null) throw ApiError.bad(400, "missing_id");
                    boolean removed = store.deleteStrategy(id);
                    logs.warn("strategy", "delete " + id + " removed=" + removed);
                    respondJson(ex, 200, new Json.Obj().put("ok", true).put("removed", removed));
                }
            });

            add("/api/settings", ex -> {
                if (method(ex, "GET")) {
                    respondJson(ex, 200, new Json.Obj().put("ok", true).put("settings", store.settings().toJson()));
                } else if (method(ex, "PUT")) {
                    Json.Obj body = readJsonObj(ex);
                    Settings s = Settings.fromJson(body.getObj("settings"), store.settings());
                    store.setSettings(s);
                    logs.info("settings", "paperFeeBps=" + s.paperFeeBps);
                    respondJson(ex, 200, new Json.Obj().put("ok", true).put("settings", s.toJson()));
                }
            });

            add("/api/backtest", ex -> {
                if (!method(ex, "POST")) return;
                Json.Obj body = readJsonObj(ex);
                BacktestRequest req = BacktestRequest.fromJson(body);
                BacktestResult res = engine.backtest(req);
                respondJson(ex, 200, new Json.Obj().put("ok", true).put("result", res.toJson()));
            });

            add("/api/logs/tail", ex -> {
                int n = qInt(ex.getRequestURI(), "n").orElse(250);
                Json.Arr arr = new Json.Arr();
                for (LogEvent ev : logs.tail(n)) arr.add(ev.toJson());
                respondJson(ex, 200, new Json.Obj().put("ok", true).put("events", arr).put("count", arr.size()));
            });

            add("/api/logs/stream", ex -> {
                if (!method(ex, "GET")) return;
                Headers h = ex.getResponseHeaders();
                h.set("Content-Type", "text/event-stream; charset=utf-8");
                h.set("Cache-Control", "no-cache, no-store, must-revalidate");
                h.set("Connection", "keep-alive");
                if (cli.cors) h.set("Access-Control-Allow-Origin", "*");
                ex.sendResponseHeaders(200, 0);
                OutputStream os = ex.getResponseBody();
                LogBus.Sub sub = logs.subscribe();
                try {
                    sse(os, "hello", new Json.Obj().put("ok", true).put("ts", System.currentTimeMillis()).toJson());
                    for (LogEvent ev : logs.tail(120)) sse(os, "log", ev.toJson().toJson());
                    os.flush();
                    while (!sub.closed.get()) {
                        LogEvent ev = sub.poll(12_000);
                        if (ev == null) {
                            sse(os, "ping", new Json.Obj().put("ts", System.currentTimeMillis()).toJson());
                            os.flush();
                            continue;
                        }
                        sse(os, "log", ev.toJson().toJson());
                        os.flush();
                    }
                } catch (IOException ignored) {
                } finally {
                    sub.close();
                    try { os.close(); } catch (Exception ignored) {}
                }
            });
        }

        private void add(String path, Handler handler) {
            server.createContext(path, ex -> {
                try {
                    if (cli.cors) corsHeaders(ex);
                    if (method(ex, "OPTIONS")) { respondEmpty(ex, 204); return; }
                    handler.handle(ex);
                } catch (ApiError ae) {
                    logs.warn("api", ae.code + " " + ae.message);
                    respondJson(ex, ae.code, new Json.Obj().put("ok", false).put("error", ae.message).put("code", ae.code));
                } catch (Throwable t) {
                    logs.error("api", "crash: " + t.getClass().getSimpleName() + ": " + t.getMessage());
                    respondJson(ex, 500, new Json.Obj().put("ok", false).put("error", "internal_error"));
                }
            });
        }

        private static void corsHeaders(HttpExchange ex) {
            Headers h = ex.getResponseHeaders();
            h.set("Access-Control-Allow-Origin", "*");
            h.set("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS");
            h.set("Access-Control-Allow-Headers", "Content-Type,Authorization");
            h.set("Access-Control-Max-Age", "86400");
        }
    }

    // -------------------------- Engine / Backtest --------------------------
    static final class Engine {
        private final Store store;
        private final LogBus logs;
        Engine(Store store, LogBus logs) { this.store = store; this.logs = logs; }

        BacktestResult backtest(BacktestRequest req) {
            StrategyDef strat = store.getStrategy(req.strategyId).orElseThrow(() -> ApiError.bad(404, "strategy_not_found"));
            Csv.ParseResult pr = Csv.parse(req.candlesCsv, req.maxBytes);
            if (!pr.ok) throw ApiError.bad(400, "bad_csv: " + pr.error);
            List<Candle> candles = pr.candles;
            if (candles.size() < 60) throw ApiError.bad(400, "need_more_candles");
            logs.info("backtest", "run " + strat.name + " candles=" + candles.size());

            Paper paper = new Paper(store.settings());
            IndicatorSet ind = new IndicatorSet(strat.rules);
            PaperState st = PaperState.initial(req.startCash, req.symbol);
            List<Fill> fills = new ArrayList<>();
            int cooldown = 0;

            for (Candle c : candles) {
                ind.update(c);
                if (!ind.ready()) continue;

                if (cooldown > 0) { cooldown--; continue; }
                Decision d = strat.rules.decide(ind);
                if (d.action == Action.BUY) {
                    paper.buy(st, c, strat.rules).ifPresent(f -> { fills.add(f); cooldown = strat.rules.cooldownBars; });
                } else if (d.action == Action.SELL) {
                    paper.sell(st, c, strat.rules).ifPresent(f -> { fills.add(f); cooldown = strat.rules.cooldownBars; });
                }
            }

            EquityCurve curve = EquityCurve.build(st, candles);
            return BacktestResult.of(req, strat, st, fills, curve, pr.warnings);
        }
    }

    enum Action { BUY, SELL, HOLD }
    static final class Decision { final Action action; final String reason; Decision(Action a, String r) { action=a; reason=r; } }

    // -------------------------- Strategy --------------------------
    static final class StrategyDef {
        final String id;
        final String name;
        final String symbol;
        final Rules rules;
        final long createdAt;
        final long updatedAt;

        StrategyDef(String id, String name, String symbol, Rules rules, long createdAt, long updatedAt) {
            this.id = id; this.name = name; this.symbol = symbol; this.rules = rules; this.createdAt = createdAt; this.updatedAt = updatedAt;
        }

        static StrategyDef defaults() {
            long now = System.currentTimeMillis();
            return new StrategyDef(
                    "s_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12),
                    "Pulse EMA-RSI",
                    "ETHUSD",
                    Rules.defaults(),
                    now,
                    now
            );
        }

        Json.Obj toJson() {
            return new Json.Obj()
                    .put("id", id)
                    .put("name", name)
                    .put("symbol", symbol)
                    .put("rules", rules.toJson())
                    .put("createdAt", createdAt)
                    .put("updatedAt", updatedAt);
        }

        static StrategyDef fromJson(Json.Obj o, StrategyDef fb) {
            if (o == null) return fb;
            long now = System.currentTimeMillis();
            String id = o.getString("id", fb == null ? null : fb.id);
            String name = o.getString("name", fb == null ? "Strategy" : fb.name);
            String symbol = o.getString("symbol", fb == null ? "ETHUSD" : fb.symbol);
            Rules rules = Rules.fromJson(o.getObj("rules"), fb == null ? Rules.defaults() : fb.rules);
            long createdAt = o.getLong("createdAt", fb == null ? now : fb.createdAt);
            if (id == null || id.isBlank()) id = "s_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
            return new StrategyDef(id, name, symbol, rules, createdAt, now);
        }
    }

    static final class Rules {
        final int emaFast;
        final int emaSlow;
        final int rsiLen;
        final double rsiBuyBelow;
        final double rsiSellAbove;
        final double riskPerTrade;
        final int slippageBps;
        final int cooldownBars;
        final double takeProfitPct;
        final double stopLossPct;

        Rules(int emaFast, int emaSlow, int rsiLen, double rsiBuyBelow, double rsiSellAbove,
              double riskPerTrade, int slippageBps, int cooldownBars, double takeProfitPct, double stopLossPct) {
            this.emaFast = emaFast;
            this.emaSlow = emaSlow;
            this.rsiLen = rsiLen;
            this.rsiBuyBelow = rsiBuyBelow;
            this.rsiSellAbove = rsiSellAbove;
            this.riskPerTrade = riskPerTrade;
            this.slippageBps = slippageBps;
            this.cooldownBars = cooldownBars;
            this.takeProfitPct = takeProfitPct;
            this.stopLossPct = stopLossPct;
        }

        static Rules defaults() {
            return new Rules(12, 26, 14, 38.0, 66.0, 0.20, 35, 3, 0.032, 0.017);
        }

        Decision decide(IndicatorSet ind) {
            boolean trendUp = ind.emaFast > ind.emaSlow;
            boolean trendDown = ind.emaFast < ind.emaSlow;
            boolean oversold = ind.rsi <= rsiBuyBelow;
            boolean overbought = ind.rsi >= rsiSellAbove;
            if (trendUp && oversold) return new Decision(Action.BUY, "trendUp+oversold");
            if (trendDown && overbought) return new Decision(Action.SELL, "trendDown+overbought");
            return new Decision(Action.HOLD, "no_edge");
        }

        Json.Obj toJson() {
            return new Json.Obj()
                    .put("emaFast", emaFast)
                    .put("emaSlow", emaSlow)
                    .put("rsiLen", rsiLen)
                    .put("rsiBuyBelow", rsiBuyBelow)
                    .put("rsiSellAbove", rsiSellAbove)
                    .put("riskPerTrade", riskPerTrade)
                    .put("slippageBps", slippageBps)
                    .put("cooldownBars", cooldownBars)
                    .put("takeProfitPct", takeProfitPct)
