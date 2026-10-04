import pandas as pd
import prices as P

D = 86400
T0 = 1_790_000_000


def census(rows):
    return pd.DataFrame(rows, columns=["pool", "mint", "dex", "pool_created_at"])


def iso(t):
    return pd.Timestamp(t, unit="s", tz="UTC").isoformat()


def log(rows):
    return pd.DataFrame(rows, columns=P.LOG_COLS)


def test_targets_graduation_and_sample():
    pf = [f"pf{i}" for i in range(400)]
    c = census([(p, f"m{p}", "pump-fun", iso(T0)) for p in pf] + [
        ("ps1", "mx", "pumpswap", iso(T0)),           # pumpswap: always
        ("met1", "mpf3", "meteora", iso(T0)),         # mint launched on pump-fun -> graduation
        ("met2", "other", "meteora", iso(T0)),        # unrelated meteora pool -> excluded
        ("ps1", "mx", "pumpswap", iso(T0)),           # duplicate sighting
    ])
    t = P.targets(c)
    assert {"ps1", "met1"} <= set(t.pool) and "met2" not in set(t.pool)
    assert t.pool.is_unique
    s = t[t.kind == "sample"]
    assert set(s.pool) == {p for p in pf if P.in_sample(p)}
    assert 8 <= len(s) <= 35                          # ~5 % of 400
    assert P.in_sample("pf7") == P.in_sample("pf7")   # deterministic


def test_plan_milestones_order_dead_expired():
    c = census([("old", "a", "pumpswap", iso(T0)), ("new", "b", "pumpswap", iso(T0 + 10 * D)),
                ("young", "c", "pumpswap", iso(T0 + 100 * D))])
    t = P.targets(c)
    now = T0 + 130 * D
    # nothing logged: first milestone for pools old enough, oldest first; 'young' (30 d) not due
    p = P.plan(t, log([]), now)
    assert [(x[0], x[1]) for x in p] == [("old", 41), ("new", 41)]
    assert p[0][2] == T0 + 41 * D and not p[0][3]
    # old: 41 ok and alive -> next due 82; new: 41 ok but last candle 35 d before window end -> dead
    lg = log([["old", 41, "", "ok", 900, T0, T0 + 40 * D, ""],
              ["new", 41, "", "ok", 50, T0 + 10 * D, T0 + 16 * D, ""]])
    assert [(x[0], x[1]) for x in P.plan(t, lg, now)] == [("old", 82)]
    # errors: retried until 3 failures, then skipped
    lg2 = log([["old", 41, "", "error", 0, "", "", ""]] * 2)
    assert P.plan(t, lg2, now)[0][:2] == ("old", 41)
    lg3 = log([["old", 41, "", "error", 0, "", "", ""]] * 3)
    assert P.plan(t, lg3, now)[0][:2] == ("old", 82)
    # window start older than the API's 180-day history -> flagged expired
    late = P.plan(t, log([]), T0 + 41 * D + 170 * D)
    assert late[0][:2] == ("old", 41) and late[0][3]


def test_empty_or_missing_marks_dead():
    t = P.targets(census([("x", "a", "pumpswap", iso(T0))]))
    assert P.plan(t, log([["x", 41, "", "empty", 0, "", "", ""]]), T0 + 200 * D) == []
    assert P.plan(t, log([["x", 41, "", "missing", 0, "", "", ""]]), T0 + 200 * D) == []


if __name__ == "__main__":
    for f in [v for k, v in dict(globals()).items() if k.startswith("test_")]:
        f()
    print("ok")
