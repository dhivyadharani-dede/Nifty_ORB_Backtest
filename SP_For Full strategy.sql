CREATE OR REPLACE PROCEDURE public.sp_run_strategy_full(
    p_strategy_name TEXT,
    p_start_date DATE,
    p_end_date DATE
)
LANGUAGE plpgsql
AS $$
DECLARE
    t_start TIMESTAMP;
BEGIN
    t_start := clock_timestamp();

    RAISE NOTICE '🚀 Starting run for strategy: % from % to %', p_strategy_name, p_start_date, p_end_date;

    --------------------------------------------------------------------
    -- 1. Apply strategy config from strategy_conditions to strategy_settings
    --------------------------------------------------------------------
    TRUNCATE public.strategy_settings;

    INSERT INTO public.strategy_settings
    SELECT strategy_name,big_candle_tf,small_candle_tf,preferred_breakout_type,breakout_threshold_pct,option_entry_price_cap,hedge_entry_price_cap,num_entry_legs,num_hedge_legs,sl_percentage,eod_time,no_of_lots,lot_size,hedge_exit_entry_ratio,hedge_exit_multiplier,leg_profit_pct,portfolio_profit_target_pct,portfolio_stop_loss_pct,portfolio_capital,max_reentry_rounds,sl_type,box_sl_trigger_pct,box_sl_hard_pct,reentry_breakout_type
    FROM public.strategy_conditions
    WHERE strategy_name = p_strategy_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Strategy % not found in strategy_conditions', p_strategy_name;
    END IF;

    RAISE NOTICE '✅ Applied strategy settings for %', p_strategy_name;

    --------------------------------------------------------------------
    -- 2. Refresh pipeline (replace/add MVs as per full workflow)
    --------------------------------------------------------------------
REFRESH MATERIALIZED VIEW mv_all_5min_breakouts;
REFRESH MATERIALIZED VIEW mv_ranked_breakouts_with_rounds;
REFRESH MATERIALIZED VIEW mv_ranked_breakouts_with_rounds_for_reentry;
REFRESH MATERIALIZED VIEW mv_base_strike_selection;
REFRESH MATERIALIZED VIEW mv_entry_and_hedge_legs;
REFRESH MATERIALIZED VIEW mv_all_legs_pnl_entry_round1;
DELETE FROM strategy_leg_book;
CALL insert_sl_legs_into_book();
REFRESH MATERIALIZED VIEW mv_reentry_triggered_breakouts;
REFRESH MATERIALIZED VIEW mv_reentry_base_strike_selection;
REFRESH MATERIALIZED VIEW mv_reentry_entry_and_hedge_legs;
REFRESH MATERIALIZED VIEW mv_all_legs_pnl_entry_round_REENTRY ;
CALL sp_run_reentry_loop(p_strategy_name);
REFRESH MATERIALIZED VIEW mv_entry_Leg_live_prices;
REFRESH MATERIALIZED VIEW mv_ALL_ENTRIES_sl_tracking_adjusted;
REFRESH MATERIALIZED VIEW mv_portfolio_mtm_pnl;
--REFRESH MATERIALIZED VIEW mv_portfolio_exit_legs;
REFRESH MATERIALIZED VIEW mv_portfolio_final_pnl;

    RAISE NOTICE '📊 Refresh complete for pipeline with %', p_strategy_name;

    --------------------------------------------------------------------
    -- 3. Store all row-level trade details
    --------------------------------------------------------------------
    -- Remove existing detailed rows for this strategy and date range
DELETE FROM public.strategy_all_results
WHERE strategy_name = p_strategy_name
  AND trade_date BETWEEN p_start_date AND p_end_date;

-- Now insert the latest rows
INSERT INTO public.strategy_all_results (
    strategy_name,
    trade_date,
    expiry_date,
    breakout_time,
    entry_time,
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
    pnl_amount,
    total_pnl_per_day
)
SELECT
    p_strategy_name,
    trade_date,
    expiry_date,
    breakout_time,
    entry_time,
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
    pnl_amount,
    total_pnl_per_day
FROM mv_portfolio_final_pnl
WHERE trade_date BETWEEN p_start_date AND p_end_date;


    --------------------------------------------------------------------
    -- 4. Store daily summary
    --------------------------------------------------------------------
   -- Remove existing daily summaries for this strategy and date range
DELETE FROM public.strategy_daily_summary
WHERE strategy_name = p_strategy_name
  AND trade_date BETWEEN p_start_date AND p_end_date;

-- Insert fresh summaries
INSERT INTO public.strategy_daily_summary (
    strategy_name,
    trade_date,
    total_daily_pnl,
    num_trades,
    avg_trade_pnl,
    max_trade_win,
    max_trade_loss
)
SELECT
    p_strategy_name,
    trade_date,
    SUM(pnl_amount) AS total_daily_pnl,
    COUNT(*) AS num_trades,
    AVG(pnl_amount) AS avg_trade_pnl,
    MAX(pnl_amount) AS max_trade_win,
    MIN(pnl_amount) AS max_trade_loss
FROM mv_portfolio_final_pnl
WHERE trade_date BETWEEN p_start_date AND p_end_date
GROUP BY trade_date;


    --------------------------------------------------------------------
    -- 5. Log run metadata
    --------------------------------------------------------------------
    INSERT INTO public.strategy_run_log (
        strategy_name,
        start_date,
        end_date,
        start_time,
        end_time,
        total_duration_sec
    )
    VALUES (
        p_strategy_name,
        p_start_date,
        p_end_date,
        t_start,
        clock_timestamp(),
        EXTRACT(EPOCH FROM clock_timestamp() - t_start)
    );

    RAISE NOTICE '🏁 Run complete for %, duration: % seconds', 
        p_strategy_name, EXTRACT(EPOCH FROM clock_timestamp() - t_start);

END;
$$;
