import pandas as pd

def make_drop_detail(products: pd.DataFrame,
                     drop_events: pd.DataFrame,
                     drop_products: pd.DataFrame) -> pd.DataFrame:
    """
    Build the unified drop_detail table used for scoring.
    Agent version of Book 03 logic.
    """

    drop_events = drop_events.copy()
    drop_events["zip_code"] = drop_events["zip_code"].astype(str)

    drop_detail = (
        drop_events[
            ["drop_id", "retailer", "source", "zip_code", "price_observed", "observed_at"]
        ]
        .merge(drop_products, on="drop_id", how="left")
        .merge(
            products[
                ["product_id", "product_name", "product_type", "exclusive_flag", "msrp"]
            ],
            on="product_id",
            how="left",
        )
    )

    return drop_detail


def build_store_reliability(store_patterns: pd.DataFrame) -> dict:
    """
    Build {(retailer, zip_code): reliability_score} lookup.
    """

    if store_patterns is None or store_patterns.empty:
        return {}

    store_patterns = store_patterns.copy()
    store_patterns["zip_code"] = store_patterns["zip_code"].astype(str)

    if "store_reliability_score" not in store_patterns.columns:
        return {}

    return {
        (row["retailer"], row["zip_code"]): float(row["store_reliability_score"])
        for _, row in store_patterns.iterrows()
    }


def score_drops(drop_detail: pd.DataFrame,
                local_zips: set,
                store_reliability: dict) -> pd.DataFrame:
    """
    Apply scoring rules to drop_detail (Book 03 logic).
    """

    def score_row(row):
        score = 0

        # Product signals
        if row.get("exclusive_flag"):
            score += 3

        if row.get("product_type") in ["ETB", "Booster Bundle"]:
            score += 2

        # Retailer signal
        if row.get("retailer") == "Pokemon Center":
            score += 2

        # Source confidence
        if row.get("source") in ["email", "app", "in_person"]:
            score += 1

        # ZIP + MSRP dominance
        try:
            if (
                str(row.get("zip_code")) in local_zips
                and float(row.get("price_observed")) <= float(row.get("msrp"))
            ):
                score += 5
        except Exception:
            pass

        # Store reliability bias (cap at +3)
        reliability = store_reliability.get(
            (row.get("retailer"), str(row.get("zip_code"))), 0
        )
        score += min(float(reliability), 3)

        return score

    scored = drop_detail.copy()
    scored["drop_score"] = scored.apply(score_row, axis=1)

    return scored.sort_values("drop_score", ascending=False)