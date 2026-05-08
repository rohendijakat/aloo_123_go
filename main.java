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
                    .put("stopLossPct", stopLossPct);
        }

        static Rules fromJson(Json.Obj o, Rules fb) {
            if (o == null) return fb;
            return new Rules(
                    o.getInt("emaFast", fb.emaFast),
                    o.getInt("emaSlow", fb.emaSlow),
                    o.getInt("rsiLen", fb.rsiLen),
                    o.getDouble("rsiBuyBelow", fb.rsiBuyBelow),
                    o.getDouble("rsiSellAbove", fb.rsiSellAbove),
                    o.getDouble("riskPerTrade", fb.riskPerTrade),
                    o.getInt("slippageBps", fb.slippageBps),
                    o.getInt("cooldownBars", fb.cooldownBars),
                    o.getDouble("takeProfitPct", fb.takeProfitPct),
                    o.getDouble("stopLossPct", fb.stopLossPct)
            );
        }
    }

    // -------------------------- Indicators --------------------------
    static final class IndicatorSet {
        final Rules rules;
        int bars;
        double emaFast = Double.NaN;
        double emaSlow = Double.NaN;
        double rsi = Double.NaN;
        double prevClose = Double.NaN;
        final Rsi rsiCalc;

        IndicatorSet(Rules rules) {
            this.rules = rules;
            this.rsiCalc = new Rsi(rules.rsiLen);
        }

        void update(Candle c) {
            bars++;
            if (Double.isNaN(emaFast)) {
                emaFast = c.close;
                emaSlow = c.close;
            } else {
                emaFast = ema(emaFast, c.close, rules.emaFast);
                emaSlow = ema(emaSlow, c.close, rules.emaSlow);
            }
            if (!Double.isNaN(prevClose)) rsi = rsiCalc.update(c.close - prevClose);
            prevClose = c.close;
        }

        boolean ready() {
            return bars > Math.max(40, Math.max(rules.emaSlow, rules.rsiLen) + 8) && !Double.isNaN(rsi);
        }

        static double ema(double prev, double price, int len) {
            double k = 2.0 / (len + 1.0);
            return prev + k * (price - prev);
        }
    }

    static final class Rsi {
        final int len;
        int n = 0;
        double avgGain = 0;
        double avgLoss = 0;
        Rsi(int len) { this.len = Math.max(2, len); }
        double update(double delta) {
            double g = Math.max(0, delta);
            double l = Math.max(0, -delta);
            n++;
            if (n <= len) {
                avgGain += g;
                avgLoss += l;
                if (n == len) { avgGain /= len; avgLoss /= len; }
            } else {
                avgGain = ((avgGain * (len - 1)) + g) / len;
                avgLoss = ((avgLoss * (len - 1)) + l) / len;
            }
            if (avgLoss == 0) return 100.0;
            double rs = avgGain / avgLoss;
            return 100.0 - (100.0 / (1.0 + rs));
        }
    }

    // -------------------------- Backtest & paper broker --------------------------
    static final class BacktestRequest {
        final String strategyId;
        final String symbol;
        final String candlesCsv;
        final int maxBytes;
        final double startCash;
        final double minTradeCash;

        BacktestRequest(String strategyId, String symbol, String candlesCsv, int maxBytes, double startCash, double minTradeCash) {
            this.strategyId = strategyId;
            this.symbol = symbol;
            this.candlesCsv = candlesCsv;
            this.maxBytes = maxBytes;
            this.startCash = startCash;
            this.minTradeCash = minTradeCash;
        }

        static BacktestRequest fromJson(Json.Obj o) {
            String sid = o.getString("strategyId", null);
            if (sid == null || sid.isBlank()) throw ApiError.bad(400, "missing_strategyId");
            String sym = o.getString("symbol", "ETHUSD");
            String csv = o.getString("candlesCsv", "");
            if (csv.isBlank()) throw ApiError.bad(400, "missing_candlesCsv");
            int max = o.getInt("maxBytes", 5_000_000);
            double cash = o.getDouble("startCash", 10_000);
            double minTrade = o.getDouble("minTradeCash", 25);
            return new BacktestRequest(sid, sym, csv, max, cash, minTrade);
        }
    }

    static final class BacktestResult {
        final String strategyId;
        final String strategyName;
        final String symbol;
        final double startCash;
        final double endCash;
        final double endEquity;
        final long trades;
        final double winRate;
        final double maxDrawdown;
        final List<Fill> fills;
        final EquityCurve curve;
        final List<String> warnings;

        BacktestResult(String strategyId, String strategyName, String symbol, double startCash, double endCash, double endEquity,
                       long trades, double winRate, double maxDrawdown, List<Fill> fills, EquityCurve curve, List<String> warnings) {
            this.strategyId = strategyId;
            this.strategyName = strategyName;
            this.symbol = symbol;
            this.startCash = startCash;
            this.endCash = endCash;
            this.endEquity = endEquity;
            this.trades = trades;
            this.winRate = winRate;
            this.maxDrawdown = maxDrawdown;
            this.fills = fills;
            this.curve = curve;
            this.warnings = warnings;
        }

        static BacktestResult of(BacktestRequest req, StrategyDef strat, PaperState st, List<Fill> fills, EquityCurve curve, List<String> warnings) {
            double endEquity = curve.points.isEmpty() ? st.cash : curve.points.get(curve.points.size() - 1).equity;
            double wins = 0;
            for (int i = 1; i < fills.size(); i++) {
                Fill a = fills.get(i - 1), b = fills.get(i);
                if (a.side.equals("BUY") && b.side.equals("SELL")) {
                    double pnl = (b.px - a.px) * b.qty;
                    if (pnl > 0) wins++;
                }
            }
            double denom = Math.max(1.0, Math.floor(fills.size() / 2.0));
            double winRate = wins / denom;
            double maxDd = computeMaxDrawdown(curve.points);
            return new BacktestResult(req.strategyId, strat.name, req.symbol, req.startCash, st.cash, endEquity, st.trades, winRate, maxDd, fills, curve, warnings);
        }

        Json.Obj toJson() {
            Json.Arr fa = new Json.Arr();
            for (Fill f : fills) fa.add(f.toJson());
            Json.Arr ca = new Json.Arr();
            for (EquityPoint p : curve.points) ca.add(p.toJson());
            Json.Arr wa = new Json.Arr();
            for (String w : warnings) wa.add(w);
            return new Json.Obj()
                    .put("strategyId", strategyId)
                    .put("strategyName", strategyName)
                    .put("symbol", symbol)
                    .put("startCash", startCash)
                    .put("endCash", endCash)
                    .put("endEquity", endEquity)
                    .put("trades", trades)
                    .put("winRate", winRate)
                    .put("maxDrawdown", maxDrawdown)
                    .put("fills", fa)
                    .put("equityCurve", ca)
                    .put("warnings", wa);
        }
    }

    static final class PaperState {
        final String symbol;
        double cash;
        double qty;
        double avgEntry;
        long trades;
        PaperState(String symbol, double cash) { this.symbol = symbol; this.cash = cash; }
        static PaperState initial(double cash, String symbol) { return new PaperState(symbol, cash); }
    }

    static final class Fill {
        final long ts;
        final String side;
        final double px;
        final double qty;
        final double fees;
        final double cashAfter;
        Fill(long ts, String side, double px, double qty, double fees, double cashAfter) {
            this.ts = ts; this.side = side; this.px = px; this.qty = qty; this.fees = fees; this.cashAfter = cashAfter;
        }
        Json.Obj toJson() {
            return new Json.Obj().put("ts", ts).put("iso", iso(ts)).put("side", side).put("px", px).put("qty", qty).put("fees", fees).put("cashAfter", cashAfter);
        }
    }

    static final class Paper {
        final Settings settings;
        Paper(Settings settings) { this.settings = settings; }

        Optional<Fill> buy(PaperState st, Candle c, Rules r) {
            if (st.qty > 0) return Optional.empty();
            double riskCash = st.cash * clamp(r.riskPerTrade, 0.01, 0.95);
            if (riskCash < 5) return Optional.empty();
            double px = c.close;
            double qty = riskCash / px;
            double fee = riskCash * (settings.paperFeeBps / 10_000.0);
            double slip = riskCash * (clamp(r.slippageBps, 0, 1000) / 10_000.0);
            double cost = riskCash + fee + slip;
            if (cost > st.cash) return Optional.empty();
            st.cash -= cost;
            st.qty = qty;
            st.avgEntry = px;
            st.trades++;
            return Optional.of(new Fill(c.ts, "BUY", px, qty, fee + slip, st.cash));
        }

        Optional<Fill> sell(PaperState st, Candle c, Rules r) {
            if (st.qty <= 0) return Optional.empty();
            double px = c.close;
            double gross = st.qty * px;
            double fee = gross * (settings.paperFeeBps / 10_000.0);
            double slip = gross * (clamp(r.slippageBps, 0, 1000) / 10_000.0);
            double net = gross - fee - slip;
            st.cash += net;
            double qty = st.qty;
            st.qty = 0;
            st.avgEntry = 0;
            st.trades++;
            return Optional.of(new Fill(c.ts, "SELL", px, qty, fee + slip, st.cash));
        }
    }

    static final class EquityPoint {
        final long ts;
        final double px;
        final double equity;
        EquityPoint(long ts, double px, double equity) { this.ts = ts; this.px = px; this.equity = equity; }
        Json.Obj toJson() { return new Json.Obj().put("ts", ts).put("px", px).put("equity", equity); }
    }

    static final class EquityCurve {
        final List<EquityPoint> points;
        EquityCurve(List<EquityPoint> points) { this.points = points; }
        static EquityCurve build(PaperState st, List<Candle> candles) {
            List<EquityPoint> pts = new ArrayList<>(candles.size());
            for (Candle c : candles) {
                double pos = st.qty * c.close;
                pts.add(new EquityPoint(c.ts, c.close, st.cash + pos));
            }
            return new EquityCurve(pts);
        }
    }

    // -------------------------- CSV --------------------------
    static final class Candle {
        final long ts;
        final double open, high, low, close, vol;
        Candle(long ts, double open, double high, double low, double close, double vol) {
            this.ts = ts; this.open=open; this.high=high; this.low=low; this.close=close; this.vol=vol;
        }
    }

    static final class Csv {
        static final class ParseResult {
            final boolean ok;
            final List<Candle> candles;
            final List<String> warnings;
            final String error;
            ParseResult(boolean ok, List<Candle> candles, List<String> warnings, String error) { this.ok=ok; this.candles=candles; this.warnings=warnings; this.error=error; }
        }

        static ParseResult parse(String csv, int maxBytes) {
            if (csv == null) return new ParseResult(false, List.of(), List.of(), "null_csv");
            if (csv.getBytes(StandardCharsets.UTF_8).length > maxBytes) return new ParseResult(false, List.of(), List.of(), "csv_too_large");
            String norm = csv.replace("\r\n", "\n").replace("\r", "\n");
            String[] lines = norm.split("\n");
            int start = 0;
            if (lines.length > 0 && lines[0].toLowerCase(Locale.ROOT).contains("open")) start = 1;
            List<Candle> out = new ArrayList<>();
            List<String> warnings = new ArrayList<>();
            long prev = -1;
            for (int i = start; i < lines.length; i++) {
                String line = lines[i].trim();
                if (line.isEmpty()) continue;
                String[] c = line.split(",");
                if (c.length < 5) return new ParseResult(false, List.of(), warnings, "bad_cols_line_" + (i + 1));
                try {
                    long ts = parseTs(c[0].trim());
                    double o = Double.parseDouble(c[1].trim());
                    double h = Double.parseDouble(c[2].trim());
                    double l = Double.parseDouble(c[3].trim());
                    double cl = Double.parseDouble(c[4].trim());
                    double v = c.length >= 6 ? Double.parseDouble(c[5].trim()) : 0.0;
                    if (prev != -1 && ts <= prev) warnings.add("non_increasing_ts_line_" + (i + 1));
                    prev = ts;
                    if (h < l) warnings.add("high_lt_low_line_" + (i + 1));
                    out.add(new Candle(ts, o, h, l, cl, v));
                } catch (Exception e) {
                    return new ParseResult(false, List.of(), warnings, "parse_error_line_" + (i + 1));
                }
            }
            if (out.size() > 200_000) warnings.add("large_dataset_" + out.size());
            return new ParseResult(true, out, warnings, null);
        }

        static long parseTs(String s) {
            if (s.matches("\\d{13}")) return Long.parseLong(s);
            if (s.matches("\\d{10}")) return Long.parseLong(s) * 1000L;
            return Instant.parse(s).toEpochMilli();
        }
    }

    // -------------------------- Settings / Store --------------------------
    static final class Settings {
        final int paperFeeBps;
        final String apiToken;
        final String note;
        Settings(int paperFeeBps, String apiToken, String note) { this.paperFeeBps=paperFeeBps; this.apiToken=apiToken; this.note=note; }
        static Settings defaults() { return new Settings(8, randomToken(), "local paper engine"); }
        Json.Obj toJson() { return new Json.Obj().put("paperFeeBps", paperFeeBps).put("apiToken", apiToken).put("note", note); }
        static Settings fromJson(Json.Obj o, Settings fb) {
            if (o == null) return fb;
            int fee = clampInt(o.getInt("paperFeeBps", fb.paperFeeBps), 0, 250);
            String tok = o.getString("apiToken", fb.apiToken);
            if (tok == null || tok.isBlank()) tok = randomToken();
            String note = o.getString("note", fb.note);
            return new Settings(fee, tok, note);
        }
    }

    static final class Store {
        private final Path dir;
        private final Path file;
        private final LogBus logs;
        private boolean dirty;
        private Settings settings;
        private final Map<String, StrategyDef> strategies = new LinkedHashMap<>();

        Store(Path dir, LogBus logs) {
            this.dir = dir;
            this.file = dir.resolve("state.json");
            this.logs = logs;
            try { Files.createDirectories(dir); } catch (IOException ignored) {}
            load();
        }

        synchronized Settings settings() { return settings; }
        synchronized void setSettings(Settings s) { settings = s; dirty = true; }
        synchronized List<StrategyDef> listStrategies() { return new ArrayList<>(strategies.values()); }
        synchronized Optional<StrategyDef> getStrategy(String id) { return Optional.ofNullable(strategies.get(id)); }
        synchronized void upsertStrategy(StrategyDef s) { strategies.put(s.id, s); dirty = true; }
        synchronized boolean deleteStrategy(String id) { StrategyDef r = strategies.remove(id); if (r != null) dirty = true; return r != null; }

        synchronized void flushIfDirty() { if (dirty) flush(); }

        private void load() {
            if (!Files.exists(file)) {
                settings = Settings.defaults();
                StrategyDef d = StrategyDef.defaults();
                strategies.put(d.id, d);
                dirty = true;
                flush();
                return;
            }
            try {
                String txt = Files.readString(file, StandardCharsets.UTF_8);
                Json.Obj root = (Json.Obj) Json.parse(txt);
                settings = Settings.fromJson(root.getObj("settings"), Settings.defaults());
                strategies.clear();
                Json.Arr arr = root.getArr("strategies");
                for (Json.Val v : arr.values) {
                    if (v instanceof Json.Obj) {
                        StrategyDef s = StrategyDef.fromJson((Json.Obj) v, null);
                        strategies.put(s.id, s);
                    }
                }
                dirty = false;
                logs.info("store", "loaded " + strategies.size() + " strategies");
            } catch (Exception e) {
                logs.error("store", "load failed, reset: " + e.getMessage());
                settings = Settings.defaults();
                strategies.clear();
                StrategyDef d = StrategyDef.defaults();
                strategies.put(d.id, d);
                dirty = true;
                flush();
            }
        }

        private void flush() {
            try {
                Json.Obj root = new Json.Obj()
                        .put("settings", settings.toJson())
                        .put("strategies", strategiesJson())
                        .put("savedAt", System.currentTimeMillis());
                Files.writeString(file, root.toPrettyString(2), StandardCharsets.UTF_8);
                dirty = false;
            } catch (Exception e) {
                logs.error("store", "flush failed: " + e.getMessage());
            }
        }

        private Json.Arr strategiesJson() {
            Json.Arr arr = new Json.Arr();
            for (StrategyDef s : strategies.values()) arr.add(s.toJson());
            return arr;
        }
    }

    // -------------------------- Logs --------------------------
    static final class LogEvent {
        final long seq;
        final long ts;
        final String level;
        final String topic;
        final String msg;
        LogEvent(long seq, long ts, String level, String topic, String msg) { this.seq=seq; this.ts=ts; this.level=level; this.topic=topic; this.msg=msg; }
        Json.Obj toJson() { return new Json.Obj().put("seq", seq).put("ts", ts).put("iso", iso(ts)).put("level", level).put("topic", topic).put("msg", msg); }
    }

    static final class LogBus {
        private final int capacity;
        private final Deque<LogEvent> ring = new ArrayDeque<>();
        private final AtomicLong seq = new AtomicLong(0);
        private final CopyOnWriteArrayList<Sub> subs = new CopyOnWriteArrayList<>();

        LogBus(int capacity) { this.capacity = Math.max(200, capacity); }
        void info(String topic, String msg) { pub("INFO", topic, msg); }
        void warn(String topic, String msg) { pub("WARN", topic, msg); }
        void error(String topic, String msg) { pub("ERROR", topic, msg); }

        void pub(String level, String topic, String msg) {
            LogEvent ev = new LogEvent(seq.incrementAndGet(), System.currentTimeMillis(), level, topic, msg);
            synchronized (ring) {
                ring.addLast(ev);
                while (ring.size() > capacity) ring.removeFirst();
            }
            for (Sub s : subs) s.offer(ev);
        }

        List<LogEvent> tail(int n) {
            n = clampInt(n, 1, capacity);
            List<LogEvent> out = new ArrayList<>(n);
            synchronized (ring) {
                int skip = Math.max(0, ring.size() - n);
                int i = 0;
                for (LogEvent ev : ring) {
                    if (i++ < skip) continue;
                    out.add(ev);
                }
            }
            return out;
        }

        Sub subscribe() { Sub s = new Sub(this); subs.add(s); return s; }
        void unsubscribe(Sub s) { subs.remove(s); }

        static final class Sub implements Closeable {
            private final LogBus bus;
            private final Deque<LogEvent> q = new ArrayDeque<>();
            final AtomicBoolean closed = new AtomicBoolean(false);
            Sub(LogBus bus) { this.bus = bus; }

            void offer(LogEvent ev) {
                synchronized (q) {
                    q.addLast(ev);
                    while (q.size() > 600) q.removeFirst();
                    q.notifyAll();
                }
            }

            LogEvent poll(long timeoutMs) {
                long end = System.currentTimeMillis() + timeoutMs;
                synchronized (q) {
                    while (!closed.get() && q.isEmpty()) {
                        long left = end - System.currentTimeMillis();
                        if (left <= 0) return null;
                        try { q.wait(left); } catch (InterruptedException ie) { return null; }
                    }
                    return q.pollFirst();
                }
            }

            @Override public void close() {
                closed.set(true);
                bus.unsubscribe(this);
                synchronized (q) { q.notifyAll(); }
            }
        }
    }

    // -------------------------- Minimal JSON --------------------------
    static final class Json {
        interface Val { String toJson(); default String toPrettyString(int indent) { return Pretty.format(toJson(), indent); } }

        static final class Null implements Val { @Override public String toJson() { return "null"; } }
        static final class Bool implements Val { final boolean v; Bool(boolean v){this.v=v;} @Override public String toJson(){return v?"true":"false";} }
        static final class Num implements Val {
            final double v;
            Num(double v){this.v=v;}
            @Override public String toJson() {
                if (Double.isNaN(v) || Double.isInfinite(v)) return "null";
                long lv = (long) v;
                if (Math.abs(v - lv) < 1e-12) return Long.toString(lv);
                return Double.toString(v);
            }
        }
        static final class Str implements Val { final String v; Str(String v){this.v=v;} @Override public String toJson(){return "\"" + esc(v) + "\"";} }
        static final class Arr implements Val {
            final List<Val> values = new ArrayList<>();
            Arr add(Object v) { values.add(wrap(v)); return this; }
            int size() { return values.size(); }
            @Override public String toJson() {
                StringBuilder sb = new StringBuilder("[");
                for (int i = 0; i < values.size(); i++) { if (i != 0) sb.append(","); sb.append(values.get(i).toJson()); }
                return sb.append("]").toString();
            }
        }
        static final class Obj implements Val {
            final Map<String, Val> map = new LinkedHashMap<>();
            Obj put(String k, Object v) { map.put(k, wrap(v)); return this; }
            Obj put(String k, Val v) { map.put(k, v == null ? new Null() : v); return this; }
            String getString(String k, String fb) { Val v = map.get(k); return v instanceof Str ? ((Str) v).v : fb; }
            int getInt(String k, int fb) { Val v = map.get(k); return v instanceof Num ? (int) ((Num) v).v : fb; }
            long getLong(String k, long fb) { Val v = map.get(k); return v instanceof Num ? (long) ((Num) v).v : fb; }
            double getDouble(String k, double fb) { Val v = map.get(k); return v instanceof Num ? ((Num) v).v : fb; }
            Obj getObj(String k) { Val v = map.get(k); return v instanceof Obj ? (Obj) v : null; }
            Arr getArr(String k) { Val v = map.get(k); return v instanceof Arr ? (Arr) v : new Arr(); }
            @Override public String toJson() {
                StringBuilder sb = new StringBuilder("{");
                int i = 0;
                for (Map.Entry<String, Val> e : map.entrySet()) {
                    if (i++ != 0) sb.append(",");
                    sb.append("\"").append(esc(e.getKey())).append("\":").append(e.getValue().toJson());
                }
                return sb.append("}").toString();
            }
        }

        static Val wrap(Object v) {
            if (v == null) return new Null();
            if (v instanceof Val) return (Val) v;
            if (v instanceof String) return new Str((String) v);
            if (v instanceof Boolean) return new Bool((Boolean) v);
            if (v instanceof Integer) return new Num(((Integer) v).doubleValue());
            if (v instanceof Long) return new Num(((Long) v).doubleValue());
            if (v instanceof Double) return new Num((Double) v);
            if (v instanceof Float) return new Num(((Float) v).doubleValue());
            return new Str(String.valueOf(v));
        }

        static Val parse(String s) { return new Parser(s).parse(); }

        static final class Parser {
            final String s;
            int i = 0;
            Parser(String s) { this.s = s == null ? "" : s.trim(); }

            Val parse() { skip(); Val v = readVal(); skip(); return v; }

            Val readVal() {
                skip();
                if (i >= s.length()) return new Null();
                char c = s.charAt(i);
                if (c == '{') return readObj();
                if (c == '[') return readArr();
                if (c == '"') return new Str(readString());
                if (eat("true")) return new Bool(true);
                if (eat("false")) return new Bool(false);
                if (eat("null")) return new Null();
                return readNum();
            }

            Obj readObj() {
                expect('{');
                Obj o = new Obj();
                skip();
                if (peek('}')) { i++; return o; }
                while (true) {
                    skip();
                    String k = readString();
                    skip();
                    expect(':');
                    Val v = readVal();
                    o.put(k, v);
                    skip();
                    if (peek('}')) { i++; break; }
                    expect(',');
                }
                return o;
            }

            Arr readArr() {
                expect('[');
                Arr a = new Arr();
                skip();
                if (peek(']')) { i++; return a; }
                while (true) {
                    a.values.add(readVal());
                    skip();
                    if (peek(']')) { i++; break; }
                    expect(',');
                }
                return a;
            }

            Num readNum() {
                int start = i;
                while (i < s.length()) {
                    char c = s.charAt(i);
                    if ((c >= '0' && c <= '9') || c == '-' || c == '.' || c == 'e' || c == 'E' || c == '+') i++;
                    else break;
