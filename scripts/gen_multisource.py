"""Multi-source commerce data generator (SIMULATION).

Models the *pattern* of a refurbished-phone reseller selling across online
marketplaces and settling cash through an order-management / logistics system.
This is a generic simulation of a common industry pattern -- NOT a reproduction
of any employer's system. All names, codes and numbers are invented.

The whole point of this shape is to force the Silver-layer skills a real Data
Engineer must show:

  * two SALES channels with DIFFERENT schemas (column names, status codes,
    currency, date formats) -> Silver must CONFORM then UNION them.
  * a SETTLEMENT source at a DIFFERENT grain (cash remitted, with fees) ->
    Silver keeps it SEPARATE; Gold reconciles it to sales by order ref.
  * reference/lookup tables (sku map, fx rate, status map) -> Silver joins them
    to translate each source into one common language.
  * deliberate volume SKEW (one channel dominates) -> a data-skew talking point.

Outputs Parquet into <out>/raw/:
    sales_amazon.parquet       channel A  (USD, string status, YYYY/MM/DD)
    sales_backmarket.parquet   channel B  (EUR, int status code, DD-MM-YYYY)
    settlements_oms.parquet     cash remitted per order (gross/fees/net, epoch date)
    ref_sku_map.parquet         (channel, source_sku) -> product_id
    ref_product.parquet         product master (cost/price/grade)  [SCD2 demo]
    ref_fx.parquet              (currency, rate_date) -> rate_to_usd  daily
    ref_status_map.parquet      (channel, raw_status) -> canonical_status

Flags:
    --scale N         total sales rows across both channels (default 1_000_000)
    --days D          spread orders over the last D days (default 120)
    --amazon-share F  fraction of sales from Amazon (default 0.78 -> skew)
    --mutate-dims     regenerate ref_product with ~8% changed price/grade
                      (run before a 2nd dbt snapshot -> SCD Type-2 history)
    --seed S          reproducibility
"""
from __future__ import annotations

import argparse
import pathlib
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

BRANDS = ["Apple", "Samsung", "Xiaomi", "Oppo", "Sony", "Google", "Generic"]
MODELS = ["Galaxy S21", "iPhone 12", "iPhone 13", "Pixel 6", "Redmi Note 11",
          "Xperia 5", "Find X3", "Galaxy A52", "iPhone SE", "Pixel 5"]
GRADES = ["A", "B", "C"]                      # refurbished cosmetic grade
GRADE_P = [0.45, 0.4, 0.15]

# canonical order statuses (the ONE language Silver conforms everything to)
CANON = ["PENDING", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"]
# raw status vocab per channel -> canonical (Silver uses ref_status_map to translate)
AMZ_STATUS = {"Pending": "PENDING", "Shipped": "SHIPPED", "Delivered": "DELIVERED",
              "Cancelled": "CANCELLED", "Returned": "RETURNED"}
BM_STATUS = {"1": "PENDING", "2": "SHIPPED", "3": "DELIVERED",
             "4": "CANCELLED", "5": "RETURNED"}
STATUS_P = [0.08, 0.12, 0.62, 0.10, 0.08]     # over the canonical set


def write(table_dict: dict, path: pathlib.Path) -> int:
    tbl = pa.table(table_dict)
    pq.write_table(tbl, path)
    return tbl.num_rows


# ---------------------------------------------------------------- reference data
def gen_products(rng, n_products: int):
    """Return (product_ids, asin[], bm_sku[], has_bm[]) and write the master."""
    ids = np.arange(1, n_products + 1, dtype="int64")
    brand_idx = rng.integers(0, len(BRANDS), n_products)
    model_idx = rng.integers(0, len(MODELS), n_products)
    grade = np.array(GRADES)[rng.choice(len(GRADES), n_products, p=GRADE_P)]
    unit_cost = np.round(rng.uniform(60, 900, n_products), 2)
    list_price = np.round(unit_cost * rng.uniform(1.2, 1.9, n_products), 2)

    # every product has an Amazon ASIN; ~60% are ALSO listed on BackMarket with a
    # DIFFERENT source sku -> the reason a sku-map exists (same product, 2 codes).
    asin = np.array([f"B0{i:08d}" for i in ids])
    has_bm = rng.random(n_products) < 0.60
    bm_sku = np.array([f"SKU{i:07d}" for i in ids])

    write(
        {
            "product_id": ids,
            "model_name": np.array(MODELS)[model_idx],
            "brand": np.array(BRANDS)[brand_idx],
            "grade": grade,
            "unit_cost": unit_cost,
            "list_price": list_price,
        },
        RAW / "ref_product.parquet",
    )
    print(f"[gen] ref_product: {n_products}")
    return ids, asin, bm_sku, has_bm


def gen_sku_map(product_ids, asin, bm_sku, has_bm):
    # channel A rows: every asin -> product_id ; channel B rows: bm sku -> product_id
    ch = np.concatenate([np.full(len(asin), "amazon"),
                         np.full(int(has_bm.sum()), "backmarket")])
    src = np.concatenate([asin, bm_sku[has_bm]])
    pid = np.concatenate([product_ids, product_ids[has_bm]])
    write({"channel": ch, "source_sku": src, "product_id": pid},
          RAW / "ref_sku_map.parquet")
    print(f"[gen] ref_sku_map: {len(ch)}")


def gen_fx(days: int):
    end = date.today()
    dates, currency, rate = [], [], []
    for d in range(days + 10):                 # a little slack for payout dates
        day = str(end - timedelta(days=d))
        for cur, base in (("USD", 1.0), ("EUR", 1.08), ("GBP", 1.27)):
            dates.append(day)
            currency.append(cur)
            if cur == "USD":
                rate.append(1.0)                       # USD->USD is exactly 1
            else:
                # tiny daily wobble so rates aren't constant
                rate.append(round(base * (1 + (hash((day, cur)) % 100 - 50) / 5000), 4))
    write({"rate_date": dates, "currency": currency, "rate_to_usd": rate},
          RAW / "ref_fx.parquet")
    print(f"[gen] ref_fx: {len(dates)}")


def gen_status_map():
    ch, raw, canon = [], [], []
    for k, v in AMZ_STATUS.items():
        ch.append("amazon"); raw.append(k); canon.append(v)
    for k, v in BM_STATUS.items():
        ch.append("backmarket"); raw.append(k); canon.append(v)
    write({"channel": ch, "raw_status": raw, "canonical_status": canon},
          RAW / "ref_status_map.parquet")
    print(f"[gen] ref_status_map: {len(ch)}")


# ------------------------------------------------------------------- sales facts
def gen_sales(rng, n_amz, n_bm, product_ids, asin, bm_sku, has_bm, days):
    end = date.today()
    bm_pids = product_ids[has_bm]
    bm_skus = bm_sku[has_bm]

    # ---- Amazon channel: USD, string status, YYYY/MM/DD dates ----
    amz_pidx = rng.integers(0, len(product_ids), n_amz)
    amz_canon = np.array(CANON)[rng.choice(len(CANON), n_amz, p=STATUS_P)]
    inv_amz = {v: k for k, v in AMZ_STATUS.items()}
    amz_raw_status = np.array([inv_amz[c] for c in CANON])[
        np.array([CANON.index(c) for c in amz_canon])]
    off = rng.integers(0, days, n_amz)
    amz_dates = np.array([(end - timedelta(days=int(d))).strftime("%Y/%m/%d")
                          for d in range(days)])[off]
    amz_ids = np.array([f"111-{i:010d}" for i in range(n_amz)])
    amz_qty = rng.integers(1, 4, n_amz).astype("int32")
    amz_price = np.round(rng.uniform(80, 1200, n_amz), 2)
    amz_gross = np.round(amz_qty * amz_price, 2)      # order value in USD
    write(
        {
            "amazon_order_id": amz_ids,
            "asin": asin[amz_pidx],
            "qty": amz_qty,
            "item_price": amz_price,
            "currency": np.full(n_amz, "USD"),
            "order_status": amz_raw_status,       # RAW words, need status-map
            "purchase_date": amz_dates,           # RAW format YYYY/MM/DD
        },
        RAW / "sales_amazon.parquet",
    )
    print(f"[gen] sales_amazon: {n_amz}")

    # ---- BackMarket channel: EUR, INT status code, DD-MM-YYYY dates ----
    bm_idx = rng.integers(0, len(bm_pids), n_bm)
    bm_canon = np.array(CANON)[rng.choice(len(CANON), n_bm, p=STATUS_P)]
    inv_bm = {v: k for k, v in BM_STATUS.items()}
    bm_state = np.array([int(inv_bm[c]) for c in CANON])[
        np.array([CANON.index(c) for c in bm_canon])].astype("int32")
    off = rng.integers(0, days, n_bm)
    bm_dates = np.array([(end - timedelta(days=int(d))).strftime("%d-%m-%Y")
                         for d in range(days)])[off]
    bm_ids = np.array([f"BM-{i:09d}" for i in range(n_bm)])
    bm_qty = rng.integers(1, 4, n_bm).astype("int32")
    bm_price = np.round(rng.uniform(70, 1050, n_bm), 2)
    bm_gross = np.round(bm_qty * bm_price, 2)          # order value in EUR
    write(
        {
            "bm_order_ref": bm_ids,
            "product_sku": bm_skus[bm_idx],
            "quantity": bm_qty,
            "unit_amount": bm_price,
            "devise": np.full(n_bm, "EUR"),
            "state": bm_state,                    # RAW int code, need status-map
            "date_commande": bm_dates,            # RAW format DD-MM-YYYY
        },
        RAW / "sales_backmarket.parquet",
    )
    print(f"[gen] sales_backmarket: {n_bm}")
    return amz_ids, amz_canon, amz_gross, bm_ids, bm_canon, bm_gross


# --------------------------------------------------------------- settlement fact
def gen_settlements(rng, amz_ids, amz_canon, amz_gross, bm_ids, bm_canon,
                    bm_gross, days):
    """Cash actually remitted per order. Different GRAIN from sales.
    Only DELIVERED/SHIPPED orders settle -> ~coverage gap Gold must handle.
    gross_amount ties to the real sale value so reconciliation is meaningful:
    net_paid = gross - marketplace_fee - shipping_fee.
    """
    end = date.today()
    rows_ref, rows_ch, gross, mkt_fee, ship_fee, net, cur, payout = \
        [], [], [], [], [], [], [], []

    def settle(ids, canon, order_gross, channel, currency, fee_rate):
        settled = np.isin(canon, ["DELIVERED", "SHIPPED"])
        idx = np.where(settled)[0]
        n = len(idx)
        g = order_gross[idx]                         # the REAL order value
        mf = np.round(g * fee_rate, 2)
        sf = np.round(rng.uniform(2, 12, n), 2)
        nt = np.round(g - mf - sf, 2)
        off = rng.integers(2, days + 5, n)
        pay = [int(datetime.combine(end - timedelta(days=int(o)),
                                    datetime.min.time(), tzinfo=timezone.utc)
                   .timestamp()) for o in off]
        rows_ref.extend(ids[idx]); rows_ch.extend([channel] * n)
        gross.extend(g); mkt_fee.extend(mf); ship_fee.extend(sf); net.extend(nt)
        cur.extend([currency] * n); payout.extend(pay)

    settle(amz_ids, amz_canon, amz_gross, "amazon", "USD", 0.15)
    settle(bm_ids, bm_canon, bm_gross, "backmarket", "EUR", 0.12)

    sid = np.array([f"STL-{i:010d}" for i in range(len(rows_ref))])
    write(
        {
            "settlement_id": sid,
            "channel": np.array(rows_ch),
            "order_ref": np.array(rows_ref),      # matches amazon_order_id / bm_order_ref
            "gross_amount": np.array(gross),
            "marketplace_fee": np.array(mkt_fee),
            "shipping_fee": np.array(ship_fee),
            "net_paid": np.array(net),
            "payout_currency": np.array(cur),
            "payout_ts": np.array(payout, dtype="int64"),   # RAW epoch seconds
        },
        RAW / "settlements_oms.parquet",
    )
    print(f"[gen] settlements_oms: {len(sid)}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scale", type=int, default=1_000_000)
    ap.add_argument("--days", type=int, default=120)
    ap.add_argument("--amazon-share", type=float, default=0.78)
    ap.add_argument("--out", default="data")
    ap.add_argument("--mutate-dims", action="store_true")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    global RAW
    RAW = pathlib.Path(args.out) / "raw"
    RAW.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(args.seed if not args.mutate_dims else args.seed + 1)

    n_products = max(500, args.scale // 250)

    if args.mutate_dims:
        # 2nd snapshot: reprice/regrade ~8% of products for SCD2 history.
        old = pq.read_table(RAW / "ref_product.parquet").to_pydict()
        n = len(old["product_id"])
        repriced = rng.random(n) < 0.08
        lp = np.array(old["list_price"])
        lp = np.where(repriced, np.round(lp * rng.uniform(0.85, 1.25, n), 2), lp)
        gr = np.array(old["grade"])
        regrade = rng.random(n) < 0.03
        gr = np.where(regrade, np.array(GRADES)[rng.integers(0, len(GRADES), n)], gr)
        write({**old, "list_price": lp, "grade": gr}, RAW / "ref_product.parquet")
        print(f"[gen] ref_product mutated for SCD2 ({n} rows)")
        return

    product_ids, asin, bm_sku, has_bm = gen_products(rng, n_products)
    gen_sku_map(product_ids, asin, bm_sku, has_bm)
    gen_fx(args.days)
    gen_status_map()

    n_amz = int(args.scale * args.amazon_share)
    n_bm = args.scale - n_amz
    amz_ids, amz_canon, amz_gross, bm_ids, bm_canon, bm_gross = gen_sales(
        rng, n_amz, n_bm, product_ids, asin, bm_sku, has_bm, args.days)
    gen_settlements(rng, amz_ids, amz_canon, amz_gross, bm_ids, bm_canon,
                    bm_gross, args.days)
    print(f"[gen] done -> {RAW}  (skew: amazon {args.amazon_share:.0%})")


if __name__ == "__main__":
    main()
