# find_growth_stocks.py
#
# iterate all fsdata files to find the stocks with the following conditions:
# - last two consecutive YoY quarterly revenue is over percentage growth revenue.
# for example, 20%
# - last two consecutive YoY quarterly op margin is over percentage growth op
# margin. for example, 20%
# if --revenue_limit is given, we filter out the stocks less than this limit.
# for example 1000e8, default value, is 1000억
# if --op_margin_limit is given, we filter out the stocks less than this limit.
# for example 100e8, default value, is 100억

import os
import argparse
import pandas as pd
import math


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pergro_revenue", type=float, required=True, help="Min % revenue growth (YoY)")
    parser.add_argument("--pergro_op_margin", type=float, required=True, help="Min % op_margin growth (YoY)")
    parser.add_argument("--num_result", type=int, required=True, help="Number of top results")
    parser.add_argument("--revenue_limit", type=float, default=1000e8, help="Minimum revenue in most recent quarter (default: 100억)")
    parser.add_argument("--op_margin_limit", type=float, default=100e8, help="Minimum op_margin in most recent quarter (default: 10억)")
    return parser.parse_args()


def get_annual_growth(df, required_quarters=2):
    df["날짜"] = pd.to_datetime(df["날짜"])
    df = df.sort_values("날짜", ascending=False).reset_index(drop=True)

    df["year"] = df["날짜"].dt.year
    df["quarter"] = df["날짜"].dt.quarter

    if len(df) < required_quarters:
        return None, None, None

    recent_rows = df.head(required_quarters)
    df = df.set_index(["year", "quarter"]).sort_index()

    rev_growths = []
    opm_growths = []
    recent_values = {"revenue": None, "op_margin": None}

    for i in range(required_quarters):
        try:
            row = recent_rows.iloc[i]
            curr_q = (row["year"], row["quarter"])
            prev_q = (row["year"] - 1, row["quarter"])

            curr = df.loc[curr_q]
            prev = df.loc[prev_q]

            if i == 0:
                recent_values["revenue"] = curr["revenue"]
                recent_values["op_margin"] = curr["op_margin"]

            if pd.notna(curr["revenue"]) and pd.notna(prev["revenue"]) and prev["revenue"] != 0:
                rev_g = 100 * (curr["revenue"] - prev["revenue"]) / abs(prev["revenue"])
                rev_growths.append(rev_g)

            if (
                pd.notna(curr["op_margin"]) and pd.notna(prev["op_margin"])
                and curr["op_margin"] > 0 and prev["op_margin"] > 0
            ):
                opm_g = 100 * (curr["op_margin"] - prev["op_margin"]) / abs(prev["op_margin"])
                opm_growths.append(opm_g)

        except KeyError:
            return None, None, None

    return rev_growths, opm_growths, recent_values


def get_latest_market_cap(df):
    df["날짜"] = pd.to_datetime(df["날짜"])
    df = df.sort_values("날짜", ascending=False)
    return df.iloc[0]["market_cap"]


def fmt(val):
    return f"{val:.2f}" if not math.isnan(val) else "NaN"


def main():
    args = parse_args()

    fs_dir = os.path.expanduser("~/hobby/quickndirty/fsdata")
    ms_dir = os.path.expanduser("~/hobby/quickndirty/msdata")

    results = []

    for fname in os.listdir(fs_dir):
        if not fname.endswith(".csv"):
            continue

        symbol = fname.replace(".csv", "")
        print(f"Processing {symbol}...")

        fs_file = os.path.join(fs_dir, fname)
        ms_file = os.path.join(ms_dir, f"{symbol}_d.csv")

        if not os.path.exists(ms_file):
            print(f"Skipping {symbol}: market cap data not found.")
            continue

        try:
            fs_df = pd.read_csv(fs_file)
            ms_df = pd.read_csv(ms_file)

            rev_growths, opm_growths, recent_vals = get_annual_growth(fs_df)
            if recent_vals is None:
                print(f"Skipping {symbol}: insufficient financial data for comparison.")
                continue

            recent_rev = recent_vals["revenue"]
            recent_opm = recent_vals["op_margin"]

            if recent_rev is not None and recent_rev < args.revenue_limit:
                print(f"Skipping {symbol}: recent revenue {recent_rev:.0f} < limit {args.revenue_limit:.0f}")
                continue
            if recent_opm is not None and recent_opm < args.op_margin_limit:
                print(f"Skipping {symbol}: recent op_margin {recent_opm:.0f} < limit {args.op_margin_limit:.0f}")
                continue

            rev_ok = rev_growths and len(rev_growths) == 2 and all(rg > args.pergro_revenue for rg in rev_growths)
            opm_ok = opm_growths and len(opm_growths) == 2 and all(og > args.pergro_op_margin for og in opm_growths)

            # Logic based on data availability
            if rev_growths and opm_growths:
                if not (rev_ok and opm_ok):
                    print(f"Skipping {symbol}: both metrics available but one or both failed thresholds (rev: {rev_growths}, opm: {opm_growths})")
                    continue
            elif rev_growths:
                if not rev_ok:
                    print(f"Skipping {symbol}: only revenue available and failed threshold (rev: {rev_growths})")
                    continue
            elif opm_growths:
                if not opm_ok:
                    print(f"Skipping {symbol}: only op_margin available and failed threshold (opm: {opm_growths})")
                    continue
            else:
                print(f"Skipping {symbol}: neither revenue nor op_margin growth data available.")
                continue

            market_cap = get_latest_market_cap(ms_df)

            rev_g1 = rev_growths[0] if rev_growths and len(rev_growths) > 0 else float("nan")
            rev_g2 = rev_growths[1] if rev_growths and len(rev_growths) > 1 else float("nan")
            opm_g1 = opm_growths[0] if opm_growths and len(opm_growths) > 0 else float("nan")
            opm_g2 = opm_growths[1] if opm_growths and len(opm_growths) > 1 else float("nan")

            results.append((symbol, rev_g1, rev_g2, opm_g1, opm_g2, market_cap))

        except Exception as e:
            print(f"Skipping {symbol}: error during processing -> {e}")
            continue

    results.sort(key=lambda x: x[-1])  # Sort by market cap

    print(f"\n{'Symbol':<10} {'Rev_G1(%)':>10} {'Rev_G2(%)':>10} {'OpM_G1(%)':>10} {'OpM_G2(%)':>10} {'MarketCap':>15}")
    for symbol, rev1, rev2, opm1, opm2, mcap in results[:args.num_result]:
        print(f"{symbol:<10} {fmt(rev1):>10} {fmt(rev2):>10} {fmt(opm1):>10} {fmt(opm2):>10} {mcap:>15,.1f}")


if __name__ == "__main__":
    main()

