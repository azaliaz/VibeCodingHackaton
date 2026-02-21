import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class Solution {

    private static class WellStats {
        double sumOil = 0.0;
        double sumWater = 0.0;
        long count = 0L;

        void add(double oil, double water) {
            sumOil += oil;
            sumWater += water;
            count++;
        }
    }

    private static class Pending {
        long ts;
        double oil;
        double water;
        double pressure;

        Pending(long ts, double oil, double water, double pressure) {
            this.ts = ts;
            this.oil = oil;
            this.water = water;
            this.pressure = pressure;
        }

        void replace(double oil, double water, double pressure) {
            this.oil = oil;
            this.water = water;
            this.pressure = pressure;
        }
    }

    private static class PressureWindowProcessor {
        final int W;
        long size = 0L; // количество ранее обработанных элементов (т.е. индекс нового = size)
        final Deque<long[]> maxDeque = new ArrayDeque<>(); // each element: {index, Double.doubleToRawLongBits(value)}
        double best = Double.NEGATIVE_INFINITY;

        PressureWindowProcessor(int W) {
            this.W = W;
        }

        // Добавляем очередное давление в поток (индекс = size)
        void add(double value) {
            long idx = size;

            // lowerBound — минимально допустимый индекс, который должен быть в префиксе
            long lowerBound = idx - (W - 1);

            // Убираем устаревшие элементы (индексы < lowerBound)
            while (!maxDeque.isEmpty()) {
                long[] front = maxDeque.peekFirst();
                if (front[0] < lowerBound) maxDeque.pollFirst();
                else break;
            }

            // Если до добавления текущего элемента уже есть хотя бы W-1 предыдущих,
            // то окно размера W (оканчивающееся в текущем элементе) полно:
            // префикс (i < j) — это индексы в deque (они принадлежат [lowerBound, idx-1]).
            if (size >= (W - 1)) {
                if (!maxDeque.isEmpty()) {
                    double curMaxPref = Double.longBitsToDouble(maxDeque.peekFirst()[1]);
                    double candidate = curMaxPref - value; // p_i - p_j, i from prefix, j = current
                    if (candidate > best) best = candidate;
                } else {
                    // Нет ни одного префиксного элемента (т.е. W==1 case или префикс пуст) — ничего не делаем
                }
            }

            // Теперь добавляем текущий элемент как потенциальный префикс для будущих элементов:
            long packed = Double.doubleToRawLongBits(value);
            while (!maxDeque.isEmpty()) {
                long[] back = maxDeque.peekLast();
                double backVal = Double.longBitsToDouble(back[1]);
                if (backVal <= value) {
                    maxDeque.pollLast();
                } else break;
            }
            maxDeque.offerLast(new long[]{idx, packed});

            // Увеличиваем счётчик (следующий добавляемый будет иметь индекс size)
            size++;
        }

        // Возвращает результат или null если insufficient data or invalid W
        Double getResultOrNull() {
            if (W <= 0) return null;            // per spec
            if (size < W) return null;          // недостаточно записей
            // если W == 1 — внутри окна нет i<j пар, по спецификации считаем 0
            if (W == 1) return 0.0;
            // Возвращаем неотрицательное значение: если best так и не обновлялся или был отрицательным, вернём 0.
            // Это соответствует новой семантике: "максимальное падение по времени" не может быть отрицательным — если падений нет, 0.
            if (best == Double.NEGATIVE_INFINITY) return 0.0;
            return Math.max(0.0, best);
        }
    }

    public static void main(String[] args) throws IOException {
        Locale.setDefault(Locale.US);
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        List<String> queries = new ArrayList<>();
        String line;
        boolean inQueries = false;
        boolean inData = false;
        while ((line = br.readLine()) != null) {
            String t = line.trim();
            if (t.isEmpty()) continue;
            if (t.equals("QUERIES")) { inQueries = true; continue; }
            if (t.equals("DATA")) { inData = true; break; }
            if (inQueries) queries.add(t);
        }

        if (!inData) {
            processQueriesAndPrint(queries, Collections.emptyMap(), Collections.emptyMap());
            return;
        }

        // Разбираем запросы PRESSURE_DROP заранее, собирая уникальные W для каждой скважины
        Map<String, Set<Integer>> pressureRequests = new HashMap<>();
        for (String q : queries) {
            if (q.startsWith("PRESSURE_DROP")) {
                String wellId = parseParam(q, "well_id");
                String wStr = parseParam(q, "window");
                if (wellId == null || wStr == null) continue;
                try {
                    int W = Integer.parseInt(wStr);
                    if (W > 0) pressureRequests.computeIfAbsent(wellId, k -> new HashSet<>()).add(W);
                } catch (Exception ignored) {}
            }
        }

        // Создаём процессоры для каждой пары (well, W)
        Map<String, List<PressureWindowProcessor>> processors = new HashMap<>();
        if (!pressureRequests.isEmpty()) {
            for (Map.Entry<String, Set<Integer>> e : pressureRequests.entrySet()) {
                String well = e.getKey();
                Set<Integer> ws = e.getValue();
                if (ws == null || ws.isEmpty()) continue;
                List<Integer> sortedWs = new ArrayList<>(ws);
                Collections.sort(sortedWs);
                List<PressureWindowProcessor> list = new ArrayList<>(sortedWs.size());
                for (int W : sortedWs) list.add(new PressureWindowProcessor(W));
                processors.put(well, list);
            }
        }

        final Map<String, Pending> pending = new HashMap<>();
        final Map<String, WellStats> stats = new HashMap<>();

        // читаем DATA в стриме — дедупликация по (well, ts) с помощью pending
        while ((line = br.readLine()) != null) {
            String trimmed = line.trim();
            if (trimmed.equals("END")) break;
            if (trimmed.isEmpty()) continue;

            String[] parts = trimmed.split(",", -1);
            if (parts.length != 5) continue;
            String well = parts[0];
            if (well == null || well.isEmpty()) continue;

            long ts;
            double oil, water, pressure;
            try {
                ts = Long.parseLong(parts[1].trim());
                oil = Double.parseDouble(parts[2].trim());
                water = Double.parseDouble(parts[3].trim());
                pressure = Double.parseDouble(parts[4].trim());
            } catch (Exception ex) {
                continue;
            }

            if (Double.isNaN(oil) || Double.isNaN(water) || Double.isInfinite(oil) || Double.isInfinite(water)) continue;
            if (oil < 0.0 || water < 0.0) continue;

            Pending p = pending.get(well);
            if (p == null) {
                pending.put(well, new Pending(ts, oil, water, pressure));
            } else {
                if (ts == p.ts) {
                    // duplicate timestamp for this well -> last wins
                    p.replace(oil, water, pressure);
                } else if (ts > p.ts) {
                    // финализируем pending запись
                    WellStats st = stats.get(well);
                    if (st == null) { st = new WellStats(); stats.put(well, st); }
                    st.add(p.oil, p.water);

                    // передаём давление в процессоры (если есть)
                    List<PressureWindowProcessor> procs = processors.get(well);
                    if (procs != null) {
                        for (PressureWindowProcessor proc : procs) proc.add(p.pressure);
                    }

                    // новый pending
                    pending.put(well, new Pending(ts, oil, water, pressure));
                } else {
                    // ts < pending.ts — в контракте для одной скважины времена не убывают => игнорируем
                    continue;
                }
            }
        }

        // финализируем оставшиеся pending
        for (Map.Entry<String, Pending> e : pending.entrySet()) {
            String well = e.getKey();
            Pending p = e.getValue();
            if (p != null) {
                WellStats st = stats.get(well);
                if (st == null) { st = new WellStats(); stats.put(well, st); }
                st.add(p.oil, p.water);

                List<PressureWindowProcessor> procs = processors.get(well);
                if (procs != null) {
                    for (PressureWindowProcessor proc : procs) proc.add(p.pressure);
                }
            }
        }

        processQueriesAndPrint(queries, stats, processors);
    }

    private static void processQueriesAndPrint(List<String> queries,
                                               Map<String, WellStats> stats,
                                               Map<String, List<PressureWindowProcessor>> processors) {
        StringBuilder out = new StringBuilder();

        for (String qLine : queries) {
            String q = qLine.trim();
            if (q.isEmpty()) {
                out.append("\n");
                continue;
            }

            if (q.startsWith("AVG_OIL")) {
                String wellId = parseParam(q, "well_id");
                if (wellId == null) { out.append("NA\n"); continue; }
                WellStats st = stats.get(wellId);
                if (st == null || st.count == 0) out.append("NA\n");
                else out.append(String.format(Locale.US, "%.6f", st.sumOil / (double) st.count)).append("\n");

            } else if (q.startsWith("WATER_CUT")) {
                String wellId = parseParam(q, "well_id");
                if (wellId == null) { out.append("NA\n"); continue; }
                WellStats st = stats.get(wellId);
                if (st == null || st.count == 0) out.append("NA\n");
                else {
                    double sumOil = st.sumOil;
                    double sumWater = st.sumWater;
                    double sumFluid = sumOil + sumWater;
                    if (sumFluid == 0.0) out.append(String.format(Locale.US, "%.6f", 0.0)).append("\n");
                    else out.append(String.format(Locale.US, "%.6f", sumWater / sumFluid)).append("\n");
                }

            } else if (q.startsWith("TOP_WELLS_BY_OIL")) {
                String kStr = parseParam(q, "k");
                if (kStr == null) { out.append("\n"); continue; }
                int k;
                try { k = Integer.parseInt(kStr); if (k <= 0) { out.append("\n"); continue; } }
                catch (Exception ex) { out.append("\n"); continue; }

                PriorityQueue<Map.Entry<String, WellStats>> pq =
                        new PriorityQueue<>((a, b) -> {
                            int cmp = Double.compare(a.getValue().sumOil, b.getValue().sumOil);
                            if (cmp != 0) return cmp;
                            return b.getKey().compareTo(a.getKey());
                        });

                for (Map.Entry<String, WellStats> entry : stats.entrySet()) {
                    WellStats st = entry.getValue();
                    if (st == null || st.count == 0) continue;
                    pq.offer(entry);
                    if (pq.size() > k) pq.poll();
                }

                if (pq.isEmpty()) { out.append("\n"); continue; }
                List<Map.Entry<String, WellStats>> result = new ArrayList<>(pq);
                result.sort((a, b) -> {
                    int cmp = Double.compare(b.getValue().sumOil, a.getValue().sumOil);
                    if (cmp != 0) return cmp;
                    return a.getKey().compareTo(b.getKey());
                });
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < result.size(); i++) {
                    if (i > 0) sb.append(',');
                    sb.append(result.get(i).getKey());
                }
                out.append(sb.toString()).append("\n");

            } else if (q.startsWith("PRESSURE_DROP")) {
                String wellId = parseParam(q, "well_id");
                String wStr = parseParam(q, "window");
                if (wellId == null || wStr == null) { out.append("NA\n"); continue; }
                int W;
                try { W = Integer.parseInt(wStr); } catch (Exception ex) { out.append("NA\n"); continue; }
                if (W <= 0) { out.append("NA\n"); continue; }

                List<PressureWindowProcessor> procs = processors.get(wellId);
                if (procs == null || procs.isEmpty()) { out.append("NA\n"); continue; }
                PressureWindowProcessor found = null;
                for (PressureWindowProcessor p : procs) if (p.W == W) { found = p; break; }
                if (found == null) { out.append("NA\n"); continue; }
                Double res = found.getResultOrNull();
                if (res == null) out.append("NA\n");
                else out.append(String.format(Locale.US, "%.6f", res.doubleValue()));
                out.append("\n");
            } else {
                out.append("\n");
            }
        }

        System.out.print(out.toString());
    }

    private static String parseParam(String q, String paramName) {
        String[] toks = q.split("\\s+");
        for (String t : toks) {
            if (t.startsWith(paramName + "=") && t.length() > paramName.length() + 1) {
                return t.substring(paramName.length() + 1);
            }
        }
        return null;
    }
}