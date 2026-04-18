import pandas as pd
import logging
from db import get_connection

logger = logging.getLogger(__name__)


def get_historical_demand(item_id: str = None, company: str = None, months_back: int = 24) -> pd.DataFrame:
    """Pull monthly aggregated demand from DEMAND_FORECAST for Prophet training."""
    filters = f"""
        WHERE ACTIVE = 1
          AND SALESQTY > 0
          AND TRY_TO_DATE(STARTDATE, 'YYYY-MM-DD"T"HH24:MI:SS.FF7') >= DATEADD('month', -{months_back}, CURRENT_DATE)
    """
    if item_id:
        filters += f" AND ITEMID = '{item_id}'"
    if company:
        filters += f" AND DATAAREAID = '{company}'"

    sql = f"""
        SELECT
            DATE_TRUNC('month', TRY_TO_DATE(STARTDATE, 'YYYY-MM-DD"T"HH24:MI:SS.FF7')) AS ds,
            SUM(SALESQTY)  AS y
        FROM DEMAND_FORECAST
        {filters}
        GROUP BY ds
        ORDER BY ds
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=["ds", "y"])
    df["ds"] = pd.to_datetime(df["ds"])
    df["y"] = df["y"].astype(float)
    return df


def run_prophet_forecast(item_id: str = None, company: str = None, periods: int = 12):
    """Fit Prophet on historical data and return forecast for next `periods` months."""
    try:
        from prophet import Prophet
    except ImportError:
        return {"error": "Prophet not installed"}

    df = get_historical_demand(item_id=item_id, company=company)

    if len(df) < 3:
        return {"error": "Not enough historical data (need at least 3 months)", "data_points": len(df)}

    df = df.dropna()
    m = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=False,
        daily_seasonality=False,
        seasonality_mode="multiplicative" if df["y"].min() > 0 else "additive",
        interval_width=0.80,
    )
    m.fit(df)

    future = m.make_future_dataframe(periods=periods, freq="MS")
    forecast = m.predict(future)

    result_df = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
    result_df["yhat"] = result_df["yhat"].clip(lower=0)
    result_df["yhat_lower"] = result_df["yhat_lower"].clip(lower=0)
    result_df["is_forecast"] = result_df["ds"] > df["ds"].max()

    historical = df.rename(columns={"y": "actual"})
    merged = result_df.merge(historical, on="ds", how="left")
    merged["ds"] = merged["ds"].dt.strftime("%Y-%m-%d")

    return {
        "data_points_used": len(df),
        "forecast_periods": periods,
        "item_id": item_id,
        "company": company,
        "series": merged.to_dict(orient="records"),
    }
