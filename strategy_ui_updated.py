import streamlit as st
import pandas as pd
import psycopg2
from io import StringIO


DB_CONFIG = {
    #'host': 'localhost',         # 👈 change if your DB is remote
    'host' : "0.tcp.in.ngrok.io",
    'dbname': 'Nifty_Data_Analysis',
    'user': 'postgres',
    'password': 'Alliswell@28',
    #'port': 5432
    'port' : 15533  
}

# Sidebar Inputs
st.sidebar.header("Strategy Settings")

strategy_name = st.sidebar.text_input("Strategy Name", value="default")
option_entry_price_cap = st.sidebar.number_input("Entry Premium Cap", value=80)
hedge_entry_price_cap = st.sidebar.number_input("Hedge Premium Cap", value=50)
preferred_breakout_type = st.sidebar.selectbox("Breakout Type", ['full_body', 'close_breakout'])
breakout_threshold = st.sidebar.number_input("Breakout Threshold", value=1.0)
num_entry_legs = st.sidebar.number_input("No. of Entry Legs", value=4)
num_hedge_legs = st.sidebar.number_input("No. of Hedge Legs", value=1)
sl_percentage = st.sidebar.number_input("Stop Loss %", value=0.2)
eod_time = st.sidebar.text_input("EOD Exit Time", value="15:20:00")
no_of_lots = st.sidebar.number_input("No. of Lots", value=1)
hedge_exit_entry_ratio = st.sidebar.number_input("Hedge Exit Entry Ratio", value=0.5)
hedge_exit_multiplier = st.sidebar.number_input("Hedge Exit Multiplier", value=3.0)
leg_profit_pct = st.sidebar.number_input("Leg Profit %", value=0.85)
portfolio_profit_target_pct = st.sidebar.number_input("Portfolio Target %", value=0.2)
portfolio_stop_loss_pct = st.sidebar.number_input("Portfolio SL %", value=0.02)
portfolio_capital = st.sidebar.number_input("Portfolio Capital", value=900000)
max_reentry_rounds = st.sidebar.number_input("Max Re-entry Rounds", value=3)

# New SL-related fields
sl_type = st.sidebar.selectbox("SL Type", ['flat', 'box'])
box_sl_trigger_pct = st.sidebar.number_input("Box SL Trigger %", value=0.2)
box_sl_hard_pct = st.sidebar.number_input("Box SL Hard SL %", value=0.5)

start_date = st.sidebar.date_input("Start Date")
end_date = st.sidebar.date_input("End Date")
min_pnl = st.sidebar.number_input("Minimum PnL", value=0.0)

if st.sidebar.button("Run Backtest"):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()


        # Upsert strategy config
        cur.execute("""
            INSERT INTO strategy_settings (
                strategy_name, option_entry_price_cap, hedge_entry_price_cap, preferred_breakout_type, breakout_threshold,
                num_entry_legs, num_hedge_legs, sl_percentage, eod_time, no_of_lots,
                hedge_exit_entry_ratio, hedge_exit_multiplier, leg_profit_pct,
                portfolio_profit_target_pct, portfolio_stop_loss_pct, portfolio_capital,
                max_reentry_rounds, sl_type, box_sl_trigger_pct, box_sl_hard_pct
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (strategy_name) DO UPDATE SET
                option_entry_price_cap = EXCLUDED.option_entry_price_cap,
                hedge_entry_price_cap = EXCLUDED.hedge_entry_price_cap,
                preferred_breakout_type = EXCLUDED.preferred_breakout_type,
                breakout_threshold = EXCLUDED.breakout_threshold,
                num_entry_legs = EXCLUDED.num_entry_legs,
                num_hedge_legs = EXCLUDED.num_hedge_legs,
                sl_percentage = EXCLUDED.sl_percentage,
                eod_time = EXCLUDED.eod_time,
                no_of_lots = EXCLUDED.no_of_lots,
                hedge_exit_entry_ratio = EXCLUDED.hedge_exit_entry_ratio,
                hedge_exit_multiplier = EXCLUDED.hedge_exit_multiplier,
                leg_profit_pct = EXCLUDED.leg_profit_pct,
                portfolio_profit_target_pct = EXCLUDED.portfolio_profit_target_pct,
                portfolio_stop_loss_pct = EXCLUDED.portfolio_stop_loss_pct,
                portfolio_capital = EXCLUDED.portfolio_capital,
                max_reentry_rounds = EXCLUDED.max_reentry_rounds,
                sl_type = EXCLUDED.sl_type,
                box_sl_trigger_pct = EXCLUDED.box_sl_trigger_pct,
                box_sl_hard_pct = EXCLUDED.box_sl_hard_pct
        """, (
                strategy_name, option_entry_price_cap, hedge_entry_price_cap, preferred_breakout_type, breakout_threshold,
                num_entry_legs, num_hedge_legs, sl_percentage, eod_time, no_of_lots,
                hedge_exit_entry_ratio, hedge_exit_multiplier, leg_profit_pct,
                portfolio_profit_target_pct, portfolio_stop_loss_pct, portfolio_capital,
                max_reentry_rounds, sl_type, box_sl_trigger_pct, box_sl_hard_pct
        ))

        conn.commit()

        # Run stored procedure
        cur.execute(f"CALL sp_run_full_backtest('{strategy_name}')")
        conn.commit()

        # Fetch result
        query = f"""
            SELECT 
        trade_date,
		expiry_date,		
        TO_CHAR(breakout_time, 'HH24:MI:SS') AS breakout_time, 
        TO_CHAR(entry_time, 'HH24:MI:SS') AS entry_time, 
        spot_price,
        option_type,
        strike,
        entry_price,
		sl_level,
		entry_round,
		leg_type,
		transaction_type,		
        exit_time,
        exit_price,
        exit_reason,
        pnl_amount            
            FROM mv_portfolio_final_pnl                      
            ORDER BY trade_date;
        """
        df = pd.read_sql(query, conn)
        st.subheader("📊 Results")
        if 'pnl_amount' in df.columns:
            df['pnl_amount'] = pd.to_numeric(df['pnl_amount'], errors='coerce')
        st.dataframe(df)

        # Portfolio Summary
        df_summary = df.groupby(['trade_date', 'expiry_date']).agg(total_pnl=('pnl_amount', 'sum')).reset_index()
        st.subheader("📊 Portfolio Summary")
        st.dataframe(df_summary)

        # PnL over time (MTM)
        mtm_df = pd.read_sql("SELECT * FROM mv_portfolio_mtm_pnl ORDER BY trade_date", conn)
        st.line_chart(mtm_df[['trade_date', 'total_pnl']].set_index('trade_date'))

        st.success("Backtest completed.")
        #st.dataframe(df)

        # CSV download
        csv = df.to_csv(index=False)
        st.download_button("Download Results as CSV", csv, "backtest_results.csv", "text/csv")

        cur.close()
        conn.close()
    except Exception as e:
        st.error(f"Error: {e}")
