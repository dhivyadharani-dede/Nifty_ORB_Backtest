import streamlit as st
import pandas as pd
import io
import psycopg2
from datetime import date
from psycopg2.extras import execute_values
from sqlalchemy import create_engine, text
import altair as alt

# ======= DATABASE CONFIG =======
DB_CONFIG = {
    'host': 'localhost',
    'dbname': 'Nifty_ORB_Backtest',
    'user': 'postgres',
    'password': 'Alliswell@28',
    'port': 5432
}
engine = create_engine(f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")

# ======= DEFAULT TEMPLATE DATA =======
template_data = [
    {
        "strategy_name": "strat_full_example",
        "big_candle_tf": 15,
        "small_candle_tf": 5,
        "preferred_breakout_type": "full_candle_breakout",
        "breakout_threshold_pct": 60,
        "option_entry_price_cap": 80,
        "hedge_entry_price_cap": 50,
        "num_entry_legs": 4,
        "num_hedge_legs": 1,
        "sl_percentage": 20,
        "eod_time": "15:20:00",
        "lot_size": 75,
        "hedge_exit_entry_ratio": 50,
        "hedge_exit_multiplier": 3,
        "leg_profit_pct": 84,
        "portfolio_profit_target_pct": 2,
        "portfolio_stop_loss_pct": 2,
        "portfolio_capital": 900000,
        "max_reentry_rounds": 1,
        "sl_type": "regular_system_sl",
        "box_sl_trigger_pct": 2,
        "box_sl_hard_pct": 2,
        "reentry_breakout_type": "full_candle_breakout"
    },
    {
        "strategy_name": "strat_pct_example",
        "big_candle_tf": 15,
        "small_candle_tf": 5,
        "preferred_breakout_type": "pct_based_breakout",
        "breakout_threshold_pct": 70,
        "option_entry_price_cap": 90,
        "hedge_entry_price_cap": 60,
        "num_entry_legs": 4,
        "num_hedge_legs": 1,
        "sl_percentage": 20,
        "eod_time": "15:29:00",
        "lot_size": 75,
        "hedge_exit_entry_ratio": 50,
        "hedge_exit_multiplier": 3,
        "leg_profit_pct": 84,
        "portfolio_profit_target_pct": 2,
        "portfolio_stop_loss_pct": 2,
        "portfolio_capital": 900000,
        "max_reentry_rounds": 2,
        "sl_type": "box_with_buffer_sl",
        "box_sl_trigger_pct": 20,
        "box_sl_hard_pct": 30,
        "reentry_breakout_type": "pct_based_breakout"
    }
]
template_df = pd.DataFrame(template_data)

# ======= STREAMLIT UI =======
st.set_page_config(page_title="Strategy Runner", layout="wide")
st.title("📊 Strategy Backtest UI")

# Step 1: Download Template
buffer = io.BytesIO()
with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
    template_df.to_excel(writer, index=False)
st.download_button(
    label="📥 Download Strategy Template",
    data=buffer,
    file_name="strategy_conditions_template.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

# Step 2: Upload File
uploaded_file = st.file_uploader("Upload completed strategy Excel", type=["xls", "xlsx"])
start_date = st.date_input("Start Date", value=date(2025, 8, 1))
end_date = st.date_input("End Date", value=date(2025, 8, 9))

if uploaded_file:
    df_upload = pd.read_excel(uploaded_file)
    st.subheader("✅ Uploaded File Preview")
    st.dataframe(df_upload)

# Step 3: Process & Run Backtest
if st.button("📤 Save to DB, Run Backtests & Download Results"):
    try:
        # Step 3.1 Save to DB
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE strategy_conditions")
        values = [tuple(v if pd.notna(v) else None for v in row) for row in df_upload.to_numpy()]
        insert_sql = f"INSERT INTO strategy_conditions ({', '.join(df_upload.columns)}) VALUES %s"
        execute_values(cur, insert_sql, values)
        conn.commit()
        st.success("✅ Strategy conditions saved to DB!")

        # Step 3.2 Run stored procedure for each strategy
        for strat in df_upload["strategy_name"]:
            st.write(f"▶ Running backtest for: {strat}")
            cur.execute(
                "CALL public.sp_run_strategy_full(%s, %s, %s)",
                (strat, start_date, end_date)
            )
            conn.commit()
        st.success("🏁 All strategies backtested successfully!")

        # Step 3.3 Download full trade logs
        output_buffer = io.BytesIO()
        with pd.ExcelWriter(output_buffer, engine="openpyxl") as writer:
            for strat in df_upload["strategy_name"]:
                cur.execute(
                    "SELECT * FROM strategy_all_results WHERE strategy_name = %s AND trade_date BETWEEN %s AND %s ORDER BY trade_date",
                    (strat, start_date, end_date)
                )
                rows = cur.fetchall()
                colnames = [desc[0] for desc in cur.description]
                result_df = pd.DataFrame(rows, columns=colnames)
                result_df.to_excel(writer, sheet_name=strat[:31], index=False)
        st.download_button(
            label="📥 Download Backtest Trade Logs (Excel)",
            data=output_buffer.getvalue(),
            file_name="backtest_trade_logs.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        summary_df = pd.read_sql(
            """
            SELECT * 
            FROM public.strategy_daily_summary
            WHERE  trade_date BETWEEN %s AND %s
            ORDER BY strategy_name, trade_date
            """,
            conn,
            params=(start_date, end_date)
        )

        st.subheader("📈 Daily Summary Results")
        st.dataframe(summary_df)

        # Step 3.5 — Plot Daily PnL
       # st.line_chart(summary_df.pivot(index="trade_date", columns="strategy_name", values="total_daily_pnl"))

        # Step 3.6 — Download CSV
        csv_buffer = io.StringIO()
        summary_df.to_csv(csv_buffer, index=False)
        st.download_button(
            label="💾 Download Summary CSV",
            data=csv_buffer.getvalue(),
            file_name="strategy_daily_summary.csv",
            mime="text/csv"
        )
        
        

        

        # Make sure the trade_date column is a datetime
        summary_df["trade_date"] = pd.to_datetime(summary_df["trade_date"])

        # Create line chart with exact dates
        chart = alt.Chart(summary_df).mark_line(point=True).encode(
            x=alt.X("trade_date:T", axis=alt.Axis(format="%Y-%m-%d", title="Trade Date", labelAngle=-45)),
            y=alt.Y("total_daily_pnl:Q", title="Total Daily PnL"),
            color=alt.Color("strategy_name:N", title="Strategy"),
            tooltip=["trade_date:T", "strategy_name:N", "total_daily_pnl:Q"]
        ).properties(
            width=800,
            height=400
        )

        st.altair_chart(chart, use_container_width=True)


        cur.close()
        conn.close()

       

    except Exception as e:
        st.error(f"Error: {e}")
