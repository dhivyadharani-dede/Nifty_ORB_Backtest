CREATE OR REPLACE FUNCTION get_heikin_ashi(interval_minutes INT)
RETURNS TABLE (
  trade_date DATE,
  candle_time TIME,
  open NUMERIC,
  high NUMERIC,
  low NUMERIC,
  close NUMERIC,
  ha_open NUMERIC,
  ha_high NUMERIC,
  ha_low NUMERIC,
  ha_close NUMERIC
)
LANGUAGE SQL
AS
$$
WITH RECURSIVE base1 AS (
  SELECT 
    date + time::time AS trade_time,
    open, high, low, close
  FROM public."Nifty50"
  WHERE time>='9:15:00'
),
with_bucket AS (
  SELECT
    date_trunc('day', trade_time) AS trade_date,
--    date_trunc('hour', trade_time) + 
--      FLOOR(EXTRACT(MINUTE FROM trade_time) / interval_minutes) * INTERVAL '1 minute' * interval_minutes AS candle_time,
  (DATE_TRUNC('day', trade_time) + INTERVAL '9 hours 15 minutes') +
    FLOOR(
      EXTRACT(EPOCH FROM (trade_time - DATE_TRUNC('day', trade_time) - INTERVAL '9 hours 15 minutes')) 
      / (interval_minutes * 60)
    ) * (interval_minutes || ' minutes')::interval AS candle_time,
    trade_time,
    open, high, low, close
  FROM base1
),
with_windowed AS (
  SELECT * ,
    FIRST_VALUE(open) OVER w AS first_open,
    LAST_VALUE(close) OVER w AS last_close
  FROM with_bucket
  WINDOW w AS (
    PARTITION BY trade_date, candle_time ORDER BY trade_time 
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  )
),
mv_nifty_candles AS (
SELECT
  trade_date,
  candle_time,
  MIN(first_open) AS open,
  MAX(high) AS high,
  MIN(low) AS low,
  MAX(last_close) AS close
FROM with_windowed
GROUP BY trade_date, candle_time
),
base AS (
  SELECT * FROM mv_nifty_candles
),
first_candle AS (
  SELECT * FROM base ORDER BY trade_date, candle_time LIMIT 1
),
recursive_ha AS (
  SELECT 
    b.trade_date,
    b.candle_time,
    b.open, b.high, b.low, b.close,
    (b.open + b.high + b.low + b.close) / 4.0 AS ha_close,
    b.open AS ha_open,
    GREATEST(b.high, b.open, (b.open + b.high + b.low + b.close)/4.0) AS ha_high,
    LEAST(b.low, b.open, (b.open + b.high + b.low + b.close)/4.0) AS ha_low
  FROM first_candle b
  UNION ALL
  SELECT 
    b.trade_date,
    b.candle_time,
    b.open, b.high, b.low, b.close,
    (b.open + b.high + b.low + b.close) / 4.0 AS ha_close,
    (r.ha_open + r.ha_close)/2.0 AS ha_open,
    GREATEST(b.high, (r.ha_open + r.ha_close)/2.0, (b.open + b.high + b.low + b.close)/4.0) AS ha_high,
    LEAST(b.low, (r.ha_open + r.ha_close)/2.0, (b.open + b.high + b.low + b.close)/4.0) AS ha_low
  FROM base b
  JOIN recursive_ha r
    ON b.candle_time = (
      SELECT MIN(c2.candle_time)
      FROM base c2
      WHERE c2.candle_time > r.candle_time
    )
)
SELECT 
  trade_date,
  candle_time::time,
  open, high, low, close,
  ROUND(ha_open, 2),
  ROUND(ha_high, 2),
  ROUND(ha_low, 2),
  ROUND(ha_close, 2)
FROM recursive_ha
ORDER BY trade_date, candle_time;
$$;
------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE refresh_mv_ha_big_candle(p_strategy_name TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    big_tf NUMERIC;
BEGIN
    -- Get inputs
    SELECT big_candle_tf
    INTO big_tf
    FROM strategy_settings
    WHERE strategy_name = p_strategy_name;

    -- Drop if exists
    EXECUTE 'DROP MATERIALIZED VIEW IF EXISTS mv_ha_big_candle';

    -- Create big candle MV
    EXECUTE format('
        CREATE MATERIALIZED VIEW mv_ha_big_candle AS
        SELECT * FROM get_heikin_ashi(%s);
    ', big_tf);

END $$;
CALL refresh_mv_ha_big_candle('default');
------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE refresh_mv_ha_small_candle(p_strategy_name TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    small_tf NUMERIC;
BEGIN
    -- Get inputs
    SELECT small_candle_tf
    INTO small_tf
    FROM strategy_settings
    WHERE strategy_name = p_strategy_name;

    -- Drop if exists
    EXECUTE 'DROP MATERIALIZED VIEW IF EXISTS mv_ha_small_candle';

   -- Create small candle MV
    EXECUTE format('
         CREATE MATERIALIZED VIEW mv_ha_small_candle AS
         SELECT * FROM get_heikin_ashi(%s);
     ', small_tf);
END $$;
CALL refresh_mv_ha_small_candle('default');
------------------------------------------------------------------------------------------------------------------------------------------------------
--DROP MATERIALIZED VIEW IF EXISTS mv_all_5min_breakouts;

CREATE MATERIALIZED VIEW mv_all_5min_breakouts AS

WITH ha_bounds AS (
	         SELECT *
				FROM (
				    SELECT 
				        trade_date,
				        candle_time,
				        ha_high,
				        ha_low,
				        ROW_NUMBER() OVER (PARTITION BY trade_date ORDER BY candle_time) AS rn
				    FROM mv_ha_big_candle
				) ranked
				WHERE rn = 1), 
		strategy_config AS (
         SELECT strategy_settings.preferred_breakout_type,
            strategy_settings.breakout_threshold_pct/100 AS breakout_threshold_pct, 
			small_candle_tf,big_candle_tf
           FROM strategy_settings
        ),
		combined AS (
         SELECT f.trade_date,
            f.candle_time,
            f.ha_open,
            f.ha_close,
            f.ha_high,
            f.ha_low,
            h.ha_high AS ha_15m_high,
            h.ha_low AS ha_15m_low,
            s.breakout_threshold_pct,
            f.ha_close - f.ha_open AS candle_body,
                CASE
                    WHEN f.ha_open > h.ha_high AND f.ha_close > h.ha_high AND f.ha_high > h.ha_high AND f.ha_low > h.ha_high THEN 'full_body_bullish'::text
                    WHEN f.ha_close > (h.ha_high + abs(f.ha_close - f.ha_open) * s.breakout_threshold_pct) AND f.ha_high > h.ha_high THEN 'pct_breakout_bullish'::text
                    WHEN f.ha_open < h.ha_low AND f.ha_close < h.ha_low AND f.ha_high < h.ha_low AND f.ha_low < h.ha_low THEN 'full_body_bearish'::text
                    WHEN f.ha_close < (h.ha_low - abs(f.ha_close - f.ha_open) * s.breakout_threshold_pct) AND f.ha_low < h.ha_low THEN 'pct_breakout_bearish'::text
                    ELSE NULL::text
                END AS breakout_type
           FROM mv_ha_small_candle f
             JOIN ha_bounds h ON f.trade_date = h.trade_date
             CROSS JOIN strategy_config s
          WHERE f.candle_time >= '9:15:00' + (s.big_candle_tf || ' minutes')::interval
        )
 SELECT trade_date,
    candle_time AS breakout_time,
    candle_time + (s.small_candle_tf || ' minutes')::interval AS entry_time,
    ha_open,
    ha_close,
    ha_high,
    ha_low,
    ha_15m_high,
    ha_15m_low,
    breakout_type
   FROM combined
   CROSS JOIN strategy_config s
   WHERE breakout_type IS NOT NULL;
  
 ------------------------------------------------------------------------------------------------------------------------------------------------------
 --DROP MATERIALIZED VIEW IF EXISTS mv_ranked_breakouts_with_rounds CASCADE;

CREATE MATERIALIZED VIEW mv_ranked_breakouts_with_rounds AS
WITH strategy AS (
    SELECT preferred_breakout_type FROM strategy_settings 
),
filtered_breakouts AS (
    SELECT *
    FROM mv_all_5min_breakouts b
    CROSS JOIN strategy s 
    WHERE b.breakout_type IS NOT NULL
        AND (
            (s.preferred_breakout_type = 'full_candle_breakout' AND b.breakout_type IN ('full_body_bullish', 'full_body_bearish')) OR
            --(s.preferred_breakout_type = 'close_breakout' AND b.breakout_type IN ('close_breakout_bullish', 'close_breakout_bearish')) OR
			(s.preferred_breakout_type = 'pct_based_breakout' AND b.breakout_type IN ('pct_breakout_bullish', 'pct_breakout_bearish','full_body_bullish', 'full_body_bearish'))         
        )
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY trade_date ORDER BY breakout_time) AS entry_round
    FROM filtered_breakouts
)
SELECT 
    trade_date,
    -- expiry_date,
    breakout_time AS breakout_time,
    (breakout_time + INTERVAL '5 minute') AS entry_time,
    breakout_type,
    CASE 
        WHEN breakout_type LIKE '%bullish%' THEN 'P'
        WHEN breakout_type LIKE '%bearish%' THEN 'C'
    END AS entry_option_type,
    entry_round
FROM ranked;
-------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE MATERIALIZED VIEW mv_ranked_breakouts_with_rounds_for_reentry AS
WITH strategy AS (
    SELECT reentry_breakout_type FROM strategy_settings 
),
filtered_breakouts AS (
    SELECT b.trade_date,b.breakout_time,b.entry_time, b.ha_open,
    b.ha_close,
    b.ha_high,
    b.ha_low,
    b.ha_15m_high,
    b.ha_15m_low,
    b.breakout_type
    FROM mv_all_5min_breakouts b
    CROSS JOIN strategy s 
	JOIN mv_ranked_breakouts_with_rounds r ON b.trade_date=r.trade_date
    WHERE b.breakout_type IS NOT NULL
        AND (
            (s.reentry_breakout_type = 'full_candle_breakout' AND b.breakout_type IN ('full_body_bullish', 'full_body_bearish')) OR
            --(s.preferred_breakout_type = 'close_breakout' AND b.breakout_type IN ('close_breakout_bullish', 'close_breakout_bearish')) OR
			(s.reentry_breakout_type = 'pct_based_breakout' AND b.breakout_type IN ('pct_breakout_bullish', 'pct_breakout_bearish','full_body_bullish', 'full_body_bearish'))         
        ) AND r.entry_round = 1 
		AND RIGHT(b.breakout_type, 7) = RIGHT(r.breakout_type, 7)
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY trade_date ORDER BY breakout_time) AS entry_round
    FROM filtered_breakouts
)
SELECT 
    trade_date,
    -- expiry_date,
    breakout_time AS breakout_time,
    (breakout_time + INTERVAL '5 minute') AS entry_time,
    breakout_type,
    CASE 
        WHEN breakout_type LIKE '%bullish%' THEN 'P'
        WHEN breakout_type LIKE '%bearish%' THEN 'C'
    END AS entry_option_type,
    entry_round
FROM ranked ;

 ------------------------------------------------------------------------------------------------------------------------------------------------------
--DROP MATERIALIZED VIEW IF EXISTS mv_base_strike_selection CASCADE;

CREATE MATERIALIZED VIEW mv_base_strike_selection AS
WITH strategy AS (
    SELECT * FROM strategy_settings 
),

-- Step 1: Use centralized breakout view with dynamic entry_round
breakout_info AS (
    SELECT
        trade_date,
        -- expiry_date,
        entry_time,
        breakout_time,
        breakout_type,
        entry_option_type,
        entry_round
    FROM mv_ranked_breakouts_with_rounds
    WHERE entry_round = 1  -- Only original (non-reentry) entries
),

-- Step 2: Get spot price at breakout time
base AS (
    SELECT 
        b.trade_date,
        -- b.expiry_date,
        b.breakout_time,
        b.entry_time,
        b.breakout_type AS breakout_direction,
        b.entry_option_type,
        b.entry_round,
        n.open AS spot_price
    FROM breakout_info b
    JOIN public."Nifty50" n
      ON n.date = b.trade_date AND n.time = b.entry_time
),

-- Step 3: Join with options to get expiry and strike candidates
base_with_expiry AS (
    SELECT 
        b.trade_date,
        o.expiry as expiry_date,
        b.breakout_time,
        b.entry_time,
        b.breakout_direction,
        b.entry_option_type,
        b.entry_round,
        o.expiry AS expiry_date_actual,
        b.spot_price
    FROM base b
    JOIN public."nifty_options_selected_data" o 
      ON o.date = b.trade_date AND o.option_type = b.entry_option_type
    GROUP BY 
        b.trade_date,o.expiry, b.breakout_time, b.entry_time,
        b.breakout_direction, b.entry_option_type, b.entry_round,
        o.expiry, b.spot_price
),
pre_ATM_calc AS (
    SELECT *, 
           ROUND(spot_price / 50.0) * 50 AS rough_atm,
           spot_price / 50.0 AS spot_div
    FROM base_with_expiry
),
atm_calc AS (
    SELECT 
        sc.*,
        CASE 
            WHEN (sc.spot_div - FLOOR(sc.spot_div)) > 0.25 THEN CEIL(sc.spot_div) * 50
            ELSE FLOOR(sc.spot_div) * 50
        END AS atm_strike
    FROM pre_ATM_calc sc
),
-- Step 4: Find strike closest to desired entry premium
base_strike_selection AS (
    SELECT 
        b.*,
        o.strike,
        o.open AS entry_price,
        s.option_entry_price_cap,
        CASE
            WHEN (
                (b.breakout_direction LIKE '%bullish%' AND o.strike > b.atm_strike)
             OR (b.breakout_direction LIKE '%bearish%' AND o.strike < b.atm_strike)
            )
            AND o.open <= s.option_entry_price_cap
            THEN 1
            ELSE 2
        END AS priority,
        ABS(o.open - s.option_entry_price_cap) AS premium_diff
    FROM atm_calc b
    JOIN public."nifty_options_selected_data" o
      ON o.date = b.trade_date 
     AND o.time = b.entry_time 
     AND o.option_type = b.entry_option_type 
     AND o.expiry = b.expiry_date_actual
    JOIN strategy s ON TRUE
),

-- Step 5: Select the best-ranked strike per day
ranked_strikes AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY trade_date, expiry_date_actual
            ORDER BY priority, premium_diff
        ) AS rn FROM base_strike_selection 
)
SELECT *
FROM ranked_strikes
WHERE rn = 1;

------------------------------------------------------------------------------------------------------------------------------------------------------
 CREATE MATERIALIZED VIEW mv_entry_and_hedge_legs AS
WITH strategy AS (
    SELECT * FROM strategy_settings 
),
entry_strike_cte AS (
    SELECT 
        o.date AS trade_date,
        o.expiry AS expiry_date,
        s.breakout_time,
        s.entry_time,
        s.breakout_direction,
        s.entry_option_type as option_type,
        s.spot_price,
        o.strike,
        o.open AS entry_price,
		s.entry_round,
        'ENTRY'::TEXT AS leg_type,
        'SELL'::TEXT AS transaction_type
    FROM mv_base_strike_selection s
    JOIN strategy st ON TRUE
    JOIN public."nifty_options_selected_data" o 
      ON o.date = s.trade_date
     AND o.expiry = s.expiry_date
     AND o.time = s.entry_time
     AND o.option_type = s.entry_option_type
     AND (
          -- For PE: increasing strike (ATM + n)
          (s.entry_option_type = 'P' AND o.strike >= s.strike)
          -- For CE: decreasing strike (ATM - n)
       OR (s.entry_option_type = 'C' AND o.strike <= s.strike)
     )
    WHERE ABS(o.strike - s.strike) <= (50 * (st.num_entry_legs - 1)) 	
),
hedge_base_strike_selection AS ( 
    SELECT 
        b.trade_date,
        b.breakout_time,
        b.entry_time,
        b.breakout_direction,
        b.expiry_date,
		b.entry_round,
        CASE 
            WHEN b.entry_option_type = 'C' THEN 'P'
            WHEN b.entry_option_type = 'P' THEN 'C'
        END AS hedge_option_type,
        b.spot_price,
        o.strike,
        o.open AS hedge_price,
        ROW_NUMBER() OVER (
            PARTITION BY b.trade_date, b.expiry_date 
            ORDER BY ABS(o.open - s.hedge_entry_price_cap)
        ) AS rn
    FROM mv_base_strike_selection b
    JOIN public."Nifty50" n 
      ON n.date = b.trade_date AND n.time = b.entry_time
    JOIN public."nifty_options_selected_data" o 
      ON o.date = b.trade_date 
     AND o.time = b.entry_time
     AND o.expiry = b.expiry_date
     AND (
        (b.entry_option_type = 'C' AND o.option_type = 'P') OR
        (b.entry_option_type = 'P' AND o.option_type = 'C')
     )
    JOIN strategy s ON TRUE
),
selected_hedge_base_strike AS (
    SELECT * FROM hedge_base_strike_selection WHERE rn = 1
),
hedge_strike_cte AS (
    SELECT 
        o.date AS trade_date,
        o.expiry AS expiry_date,
        s.breakout_time,
        s.entry_time,
        s.breakout_direction,
        s.hedge_option_type as option_type,
        s.spot_price,
        o.strike,
        o.open AS entry_price,
		s.entry_round,
        'HEDGE'::TEXT AS leg_type,
        'SELL'::TEXT AS transaction_type
    FROM selected_hedge_base_strike s
    JOIN strategy st ON TRUE
    JOIN public."nifty_options_selected_data" o 
      ON o.date = s.trade_date
     AND o.expiry = s.expiry_date
     AND o.time = s.entry_time
     AND o.option_type = s.hedge_option_type
     AND (
          -- For PE: increasing strike (ATM + n)
          (s.hedge_option_type = 'P' AND o.strike <= s.strike)
          -- For CE: decreasing strike (ATM - n)
       OR (s.hedge_option_type = 'C' AND o.strike >= s.strike)
     )
    WHERE ABS(o.strike - s.strike) <= (50 * (st.num_hedge_legs - 1))
)
SELECT * FROM entry_strike_cte
-- WHERE trade_date='2025-06-19'
UNION ALL
SELECT * FROM hedge_strike_cte
-- WHERE trade_date='2025-06-19'
ORDER BY trade_date,expiry_date, leg_type, strike;

------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE MATERIALIZED VIEW mv_all_legs_pnl_entry_round1 AS

WITH strategy AS (
    SELECT 
        sl_type,preferred_breakout_type,
		ROUND(breakout_threshold_pct/100,3) AS breakout_threshold_pct,
        ROUND(sl_percentage/100,3) AS sl_percentage,
        ROUND(box_sl_trigger_pct/100,3) AS box_sl_trigger_pct,
        ROUND(box_sl_hard_pct/100,3) AS box_sl_hard_pct,
		ROUND(hedge_exit_entry_ratio/100,3) AS hedge_exit_entry_ratio,
		hedge_exit_multiplier,
		ROUND(leg_profit_pct/100,3) AS leg_profit_pct,
        eod_time,
        no_of_lots,lot_size
    FROM strategy_settings
),
legs AS (
    SELECT 
    e.*,
	CASE 
		WHEN e.leg_type = 'ENTRY' THEN ROUND(e.entry_price * (1 + s.sl_percentage), 2) 
		ELSE 0
	END	AS sl_level
	FROM mv_entry_and_hedge_legs e
	CROSS JOIN strategy s 
	-- WHERE e.leg_type = 'ENTRY'
),
nifty_range AS (
    SELECT *
				FROM (
				    SELECT 
				        trade_date,
				        candle_time,
				        ha_high AS breakout_high,
				        ha_low AS breakout_low,
				        ROW_NUMBER() OVER (PARTITION BY trade_date ORDER BY candle_time) AS rn
				    FROM mv_ha_big_candle
				) ranked
				WHERE rn = 1  -- assumed to contain 9:15 breakout range
),
live_prices AS (
    SELECT 
        l.*,
        o.time AS ltp_time,
        o.high AS option_high,
		o.open AS option_open,
		o.close AS option_close,
        n.high AS nifty_high,
        n.low AS nifty_low,
        n.time AS nifty_time
    FROM legs l
    JOIN strategy c ON TRUE
    JOIN public.nifty_options_selected_data o
      ON o.date = l.trade_date AND o.expiry = l.expiry_date
     AND o.option_type = l.option_type AND o.strike = l.strike
     AND o.time > l.entry_time
	 AND o.time<=c.eod_time
    JOIN public."Nifty50" n
      ON n.date = l.trade_date AND n.time = o.time
    JOIN nifty_range b
      ON b.trade_date = l.trade_date
),
sl_level_calc AS(
 SELECT DISTINCT ON (trade_date, expiry_date, option_type, strike, entry_round)
        lp.trade_date,
        expiry_date,
        breakout_time,
        entry_time,
        spot_price,
        option_type,
        strike,
        entry_price,
		CASE 
        	WHEN c.sl_type = 'regular_system_sl' THEN ROUND(entry_price * (1 + c.sl_percentage), 2)
        	WHEN c.sl_type = 'box_with_buffer_sl' THEN ROUND(entry_price * (1 + c.box_sl_hard_pct), 2)
        END AS sl_level,
		entry_round,
        leg_type,
        transaction_type       
    FROM live_prices lp
	JOIN nifty_range nr ON lp.trade_date=nr.trade_date
    JOIN strategy c ON TRUE
    WHERE leg_type='ENTRY' 
    ORDER BY trade_date, expiry_date, option_type, strike, entry_round, ltp_time
),
sl_triggered AS (
    SELECT DISTINCT ON (trade_date, expiry_date, option_type, strike, entry_round)
        lp.trade_date,
        lp.expiry_date,
        lp.breakout_time,
        lp.entry_time,
        lp.spot_price,
        lp.option_type,
        lp.strike,
        lp.entry_price,
		slc.sl_level,
		lp.entry_round,
        lp.leg_type,
        lp.transaction_type,       
        lp.ltp_time AS exit_time,
        slc.sl_level AS exit_price,
        'SL Hit' AS exit_reason,
        ROUND((lp.entry_price - slc.sl_level) * lot_size * c.no_of_lots, 2) AS pnl_amount
		-- lp.nifty_high , nr.breakout_high,
		-- lp.nifty_low , nr.breakout_low
    FROM sl_level_calc slc
	JOIN  live_prices lp ON lp.trade_date=slc.trade_date AND lp.entry_time=slc.entry_time
	AND lp.strike=slc.strike
	JOIN nifty_range nr ON lp.trade_date=nr.trade_date
    JOIN strategy c ON TRUE
    WHERE lp.leg_type='ENTRY' AND
        (
            -- Flat SL Logic
			lp.leg_type='ENTRY' AND
            c.sl_type = 'regular_system_sl' AND lp.option_high >= ROUND(lp.entry_price * (1 + c.sl_percentage), 2)
        )
        OR (
            -- Box SL Logic: if hard SL breached
			lp.leg_type='ENTRY' AND
            c.sl_type = 'box_with_buffer_sl' AND lp.option_high >= ROUND(lp.entry_price * (1 + c.box_sl_hard_pct), 2)
        )
        OR (
            -- Box SL Logic: above 20% but Nifty breaks the box
			lp.leg_type='ENTRY' AND
            c.sl_type = 'box_with_buffer_sl'
            AND lp.option_high >= ROUND(lp.entry_price * (1 + c.box_sl_trigger_pct), 2)
            AND (        -- full_candle_breakout logic
        (c.preferred_breakout_type = 'full_candle_breakout'
         AND (
             (lp.option_type = 'P' AND lp.nifty_high < nr.breakout_high)
             OR
             (lp.option_type = 'C' AND lp.nifty_low > nr.breakout_low)
         )
        )
         OR

        -- pct_based logic
        (c.preferred_breakout_type = 'pct_based_breakout'
         AND (
             (lp.option_type = 'P' AND lp.nifty_high < nr.breakout_high * (1 - c.breakout_threshold_pct))
             OR
             (lp.option_type = 'C' AND lp.nifty_low > nr.breakout_low * (1 + c.breakout_threshold_pct))
         )
        )))
    ORDER BY trade_date, expiry_date, option_type, strike, entry_round, ltp_time
),
sl_leg_counts AS (
    SELECT 
        l.trade_date,
        l.expiry_date,
        l.entry_round,
        COUNT(*) FILTER (WHERE l.leg_type = 'ENTRY') AS total_entry_legs,
        COUNT(*) FILTER (WHERE sl.leg_type = 'ENTRY' AND sl.exit_reason = 'SL Hit') AS sl_hit_legs,
        SUM(lp.option_open) FILTER (WHERE l.leg_type = 'ENTRY') AS total_entry_ltp,
        MAX(lp.option_open) FILTER (WHERE l.leg_type = 'HEDGE') AS hedge_ltp
    FROM legs l 
    LEFT JOIN sl_triggered sl 
        ON sl.trade_date = l.trade_date 
        AND sl.expiry_date = l.expiry_date
        AND sl.option_type = l.option_type 
        AND sl.strike = l.strike 
        AND l.entry_round = sl.entry_round
    LEFT JOIN live_prices lp 
        ON lp.trade_date = l.trade_date 
        AND lp.expiry_date = l.expiry_date
        AND lp.option_type = l.option_type 
        AND lp.strike = l.strike 
        AND lp.entry_round = l.entry_round
    GROUP BY l.trade_date, l.expiry_date, l.entry_round
),
hedge_exit_on_ALL_SL AS (
    SELECT 
        h.trade_date,
		h.expiry_date,
        h.breakout_time,
		h.entry_time,
		h.spot_price,
        h.option_type,
        h.strike,
        h.entry_price,
		0 AS sl_level,
		h.entry_round,
        'HEDGE' AS leg_type,
        'SELL' AS transaction_type,
        MAX(e.exit_time) AS exit_time,
		h.option_high AS exit_price,
        'ALL ENTRY SL' AS exit_reason,
        ROUND((h.entry_price - h.option_high) * c.no_of_lots * c.lot_size, 2) AS pnl_amount
    FROM live_prices h
    JOIN sl_leg_counts d ON h.trade_date = d.trade_date AND h.entry_round=d.entry_round
	AND h.expiry_date=d.expiry_date
    JOIN sl_triggered e 
	CROSS JOIN strategy c
      ON e.trade_date = h.trade_date AND e.expiry_date=h.expiry_date
	  AND e.leg_type = 'ENTRY' AND e.exit_reason = 'SL Hit' AND h.entry_round=e.entry_round	   
    WHERE h.leg_type = 'HEDGE' AND d.total_entry_legs=d.sl_hit_legs 
    GROUP BY h.trade_date,h.expiry_date, h.option_type, h.strike, h.entry_price, h.breakout_time,
	h.entry_time,h.spot_price,h.entry_round,h.option_high,c.no_of_lots,c.lot_size,h.ltp_time
	HAVING MAX(e.exit_time) = h.ltp_time
),
rehedge_candidate AS (
    SELECT 
        h.trade_date,
		h.expiry_date,
		h.breakout_time,
        h.exit_time + INTERVAL '1 minute' AS entry_time,
		h.spot_price,
        CASE WHEN h.option_type = 'C' THEN 'P' ELSE 'C' END AS option_type,
        o.strike,
        o.open AS entry_price,
        ABS(o.open - h.exit_price) AS premium_diff,
        h.exit_price,
		h.entry_round,
        'REHEDGE' AS leg_type,
        'SELL' AS transaction_type,
        0 AS pnl_amount,
        'REENTRY ON SL' AS exit_status
    FROM hedge_exit_on_ALL_SL h
    JOIN public."nifty_options_selected_data" o
      ON o.date = h.trade_date 
	 AND o.expiry=h.expiry_date
     AND o.option_type = CASE WHEN h.option_type = 'C' THEN 'P' ELSE 'C' END
     AND o.time = (h.exit_time+ INTERVAL '1 minute')
),
ranked_rehedge AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY trade_date,expiry_date ORDER BY premium_diff) AS rn
    FROM rehedge_candidate
),
rehedge_leg AS (
    SELECT * FROM ranked_rehedge WHERE rn = 1
),
rehedge_eod_exit AS (
SELECT DISTINCT ON (h.trade_date, h.expiry_date, h.option_type, h.strike)
        h.trade_date,
		h.expiry_date,
		h.breakout_time,
        h.entry_time,
		h.spot_price,
        h.option_type::char,
        h.strike,
        h.entry_price,
		0 AS sl_level,
		h.entry_round,
		h.leg_type,
        'SELL'::TEXT AS transaction_type,
        sc.eod_time::TIME AS exit_time,
        o.open AS exit_price,
		'EOD CLOSE' AS exit_status,
        ROUND((h.entry_price - o.open) * sc.no_of_lots * sc.lot_size, 2) AS pnl_amount
    FROM rehedge_leg h
    JOIN strategy sc ON TRUE
    JOIN public."nifty_options_selected_data" o 
      ON o.date = h.trade_date 
	 AND o.expiry = h.expiry_date 
     AND o.option_type = h.option_type 
     AND o.strike = h.strike
     AND o.time::TIME = sc.eod_time::TIME
    -- WHERE h.leg_type = 'REHEDGE'
),
hedge_exit_50pct_NOSL AS (
  SELECT DISTINCT ON (h.trade_date, h.expiry_date, h.option_type, h.strike,h.entry_round)
    sl.trade_date,
    sl.expiry_date,
    h.breakout_time,
    h.entry_time,
    h.spot_price,
    h.option_type::char,
    h.strike,
    h.entry_price,
    sc.hedge_exit_entry_ratio * sl.total_entry_ltp AS sl_level,
    h.entry_round,
    h.leg_type,
    'SELL'::TEXT AS transaction_type,
    h.ltp_time::TIME AS exit_time,
    h.option_open AS sl_hit_price,
    'EXIT - 50% ENTRY < HEDGE' AS exit_status,
    ROUND((h.entry_price - h.option_open) * sc.no_of_lots * sc.lot_size, 2) AS pnl_amount,
    ROW_NUMBER() OVER (
        PARTITION BY sl.trade_date, sl.expiry_date, h.option_type, h.strike, h.entry_round 
        ORDER BY h.ltp_time
    ) AS rn
FROM live_prices h
JOIN sl_leg_counts sl
    ON sl.trade_date = h.trade_date 
   AND sl.expiry_date = h.expiry_date 
   AND sl.entry_round = h.entry_round
CROSS JOIN strategy sc
WHERE sl.sl_hit_legs = 0
  AND sl.total_entry_ltp IS NOT NULL
  AND sl.hedge_ltp IS NOT NULL
  AND sl.hedge_ltp > sc.hedge_exit_entry_ratio * sl.total_entry_ltp
  ),
hedge_exit_3x_Min_one_SL AS (
  SELECT DISTINCT ON (h.trade_date, h.expiry_date, h.option_type, h.strike,h.entry_round) 
    sl.trade_date,
    sl.expiry_date,
    h.breakout_time,
    h.entry_time,
    h.spot_price,
    h.option_type::char,
    h.strike,
    h.entry_price,
    sc.hedge_exit_multiplier * sl.total_entry_ltp AS sl_level,
    h.entry_round,
    h.leg_type,
    'SELL'::TEXT AS transaction_type,
    h.ltp_time::TIME AS exit_time,
    h.option_open AS sl_hit_price,
    'EXIT - 50% ENTRY < HEDGE' AS exit_status,
    ROUND((h.entry_price - h.option_open) * sc.no_of_lots * sc.lot_size, 2) AS pnl_amount,
    ROW_NUMBER() OVER (
        PARTITION BY sl.trade_date, sl.expiry_date, h.option_type, h.strike, h.entry_round 
        ORDER BY h.ltp_time
    ) AS rn
FROM live_prices h
JOIN sl_leg_counts sl
    ON sl.trade_date = h.trade_date 
   AND sl.expiry_date = h.expiry_date 
   AND sl.entry_round = h.entry_round
CROSS JOIN strategy sc
WHERE sl.sl_hit_legs >1 AND sl.sl_hit_legs!=sl.total_entry_legs
  AND sl.total_entry_ltp IS NOT NULL
  AND sl.hedge_ltp IS NOT NULL
  AND sl.hedge_ltp > sc.hedge_exit_multiplier * sl.total_entry_ltp),
closed_legs AS (
  SELECT * FROM sl_triggered
  UNION ALL 
  SELECT * FROM  hedge_exit_on_ALL_SL
  UNION ALL 
  SELECT * FROM  rehedge_eod_exit
  UNION ALL 
  SELECT trade_date,expiry_date,breakout_time,entry_time,spot_price,option_type,strike,entry_price,
    sl_level,entry_round,leg_type,transaction_type,exit_time,sl_hit_price,exit_status,pnl_amount
  FROM hedge_exit_50pct_NOSL WHERE rn=1
  UNION ALL
  SELECT trade_date,expiry_date,breakout_time,entry_time,spot_price,option_type,strike,entry_price,
    sl_level,entry_round,leg_type,transaction_type,exit_time,sl_hit_price,exit_status,pnl_amount
  FROM hedge_exit_3x_Min_one_SL WHERE rn=1
  ),
profit_booking_entry AS (
    SELECT DISTINCT ON (l.trade_date, l.expiry_date, l.option_type, l.strike,l.entry_round)
        l.trade_date,
        l.expiry_date,
		l.breakout_time,
        l.entry_time,
		l.spot_price,
        l.option_type::char,
        l.strike,
        l.entry_price,
        ROUND(l.entry_price * (1 + c.sl_percentage), 2) AS sl_level,
		l.entry_round,
        l.leg_type,
        l.transaction_type,		
        o.time AS exit_time,
        o.open AS exit_price,
        'Profit Booking ' AS exit_reason,
        ROUND((l.entry_price - o.open) * lot_size * c.no_of_lots, 2) AS pnl_amount
    FROM legs l
    JOIN strategy c ON TRUE
    JOIN public."nifty_options_selected_data" o
        ON o.date = l.trade_date AND o.expiry = l.expiry_date
       AND o.option_type = l.option_type AND o.strike = l.strike
    WHERE o.time > l.entry_time
      AND o.open <= ROUND(l.entry_price * (1-c.leg_profit_pct), 2)
	   AND NOT EXISTS (
        SELECT 1 FROM closed_legs s
        WHERE s.trade_date = l.trade_date
          AND s.expiry_date = l.expiry_date
          AND s.option_type = l.option_type
          AND s.strike = l.strike
          AND s.entry_round = l.entry_round
      )
    ORDER BY l.trade_date, l.expiry_date, l.option_type, l.strike,l.entry_round, o.time
),
eod_close AS (
    SELECT 
        l.trade_date,
		l.expiry_date,
		l.breakout_time,
        l.entry_time,
        l.spot_price,
        l.option_type,
        l.strike,
        l.entry_price,
		CASE 
        	WHEN c.sl_type = 'regular_system_sl' THEN ROUND(l.entry_price * (1 + c.sl_percentage), 2)
        	WHEN c.sl_type = 'box_with_buffer_sl' THEN ROUND(l.entry_price * (1 + c.box_sl_hard_pct), 2)
        END AS sl_level,
		l.entry_round,
		l.leg_type,
		l.transaction_type,		
        c.eod_time AS exit_time,
        o.open AS exit_price,
        -- ROUND(l.entry_price * (1 + c.sl_percentage), 2) AS sl_level,
        'EOD CLOSE' AS exit_reason,
        ROUND((l.entry_price - o.open) * lot_size * c.no_of_lots, 2) AS pnl_amount
    FROM legs l
    JOIN strategy c ON TRUE
    JOIN public.nifty_options_selected_data o
      ON o.date = l.trade_date AND o.expiry = l.expiry_date
     AND o.option_type = l.option_type AND o.strike = l.strike
     AND o.time::TIME = c.eod_time::TIME
    WHERE NOT EXISTS (
        SELECT 1 FROM closed_legs s
        WHERE s.trade_date = l.trade_date
          AND s.expiry_date = l.expiry_date
          AND s.option_type = l.option_type
          AND s.strike = l.strike
          AND s.entry_round = l.entry_round
    )
	AND NOT EXISTS (
        SELECT 1 FROM profit_booking_entry s
        WHERE s.trade_date = l.trade_date
          AND s.expiry_date = l.expiry_date
          AND s.option_type = l.option_type
          AND s.strike = l.strike
          AND s.entry_round = l.entry_round
      )
),
double_buy_legs AS (
SELECT 
    s.trade_date,
    s.expiry_date,
    s.breakout_time,
    s.exit_time AS entry_time,
    s.spot_price,
    s.option_type::char,
    s.strike,
    s.exit_price AS entry_price,
    0 AS sl_level,
	 s.entry_round,
    'DOUBLE BUY' AS leg_type,
    'BUY' AS transaction_type,
    c.eod_time AS exit_time,
    o.open AS exit_price,
	 'EOD EXIT' AS exit_reason,
    -- CASE 
    --     WHEN n.next_entry_time IS NOT NULL THEN 'Exit for Reentry'
    --     ELSE 'EOD EXIT'
    -- END AS exit_reason,
	ROUND((s.exit_price - o.open) * c.lot_size * c.no_of_lots, 2) AS pnl_amount
    --ROUND((o.open - s.exit_price) * c.lot_size * c.no_of_lots, 2) AS pnl_amount
FROM sl_triggered s
JOIN strategy c ON TRUE
-- LEFT JOIN next_entry_time n 
--   ON s.trade_date = n.trade_date 
--  AND s.expiry_date = n.expiry_date 
--  AND s.entry_round + 1 = n.entry_round
JOIN public.nifty_options_selected_data o 
  ON o.date = s.trade_date
 AND o.expiry = s.expiry_date
 AND o.option_type = s.option_type
 AND o.strike = s.strike
 AND o.time = c.eod_time
 ORDER BY s.trade_date,s.expiry_date )
 
SELECT * FROM closed_legs
UNION ALL
SELECT * FROM profit_booking_entry
UNION ALL
SELECT * FROM eod_close
UNION ALL
SELECT * FROM double_buy_legs
ORDER BY trade_date, expiry_date, entry_time, exit_time,strike,leg_type, entry_round;
-------------------------------------------------------------------------------------------------------------------------------------------------------
DELETE FROM strategy_leg_book;
------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE insert_sl_legs_into_book()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO strategy_leg_book (
        trade_date,
        expiry_date,
        breakout_time,
        entry_time,
        exit_time,
        option_type,
        strike,
        entry_price,
        exit_price,
        transaction_type,
        leg_type,
        entry_round,
        exit_reason
    )
    SELECT 
        trade_date,
        expiry_date,
        breakout_time,
        entry_time,
        exit_time,
        option_type,
        strike,
        entry_price,
        exit_price,
        transaction_type,
        leg_type,
        entry_round,
        exit_reason
    FROM mv_all_legs_pnl_entry_round1 sl
    WHERE NOT EXISTS (
        SELECT 1
        FROM strategy_leg_book b
        WHERE b.trade_date = sl.trade_date
          AND b.expiry_date = sl.expiry_date
          AND b.option_type = sl.option_type
          AND b.strike = sl.strike
          AND b.entry_round = sl.entry_round
          AND b.leg_type = sl.leg_type
    );

    RAISE NOTICE '✅ SL legs inserted into strategy_leg_book';
END;
$$;


CALL insert_sl_legs_into_book();
------------------------------------------------------------------------------------------------------------------------------------------------------

-- DROP MATERIALIZED VIEW IF EXISTS mv_reentry_triggered_breakouts CASCADE;

CREATE MATERIALIZED VIEW mv_reentry_triggered_breakouts AS
WITH config AS (
    SELECT max_reentry_rounds,reentry_breakout_type FROM strategy_settings 
),

-- Step 1: Get SL hit time for each entry_round
sl_hit_info AS (
    SELECT 
        trade_date,
        expiry_date,
        entry_round,
        MIN(exit_time) AS last_sl_exit_time
    FROM strategy_leg_book
    WHERE exit_reason = 'SL Hit'
    GROUP BY trade_date, expiry_date, entry_round
),

-- Step 2: Determine when to start scanning for next breakout
scan_start_time AS (
    SELECT 
        s.trade_date,
        s.expiry_date,
        s.entry_round + 1 AS next_entry_round,
		-- s.last_sl_exit_time,
       -- s.last_sl_exit_time + INTERVAL '5 minutes' AS scan_start_time,
		date_trunc('hour', s.last_sl_exit_time) 
+ INTERVAL '1 minute' * CEIL(EXTRACT(MINUTE FROM s.last_sl_exit_time)::INT / 5.0) * 5 AS scan_start_time,
        c.max_reentry_rounds
    FROM sl_hit_info s
    JOIN config c ON TRUE
    WHERE s.entry_round <= c.max_reentry_rounds
),

-- Step 3: From ranked breakout view, pick the next breakout after SL
ranked_next_breakouts AS (
    SELECT 
        b.*,
        s.next_entry_round,
        ROW_NUMBER() OVER (
            PARTITION BY b.trade_date 
            ORDER BY b.breakout_time
        ) AS rn_all,  -- global ranking (optional)
        ROW_NUMBER() OVER (
            PARTITION BY s.trade_date, s.next_entry_round 
            ORDER BY b.breakout_time
        ) AS rn  -- reentry round-specific ranking
    FROM mv_ranked_breakouts_with_rounds_for_reentry b
    JOIN scan_start_time s
      ON b.trade_date = s.trade_date
     AND b.breakout_time >= s.scan_start_time
)

-- Step 4: Pick first breakout after SL per reentry round
SELECT 
    trade_date,
    -- expiry_date,
    breakout_time,
    --breakout_time AS entry_time,
	breakout_time + INTERVAL '5 minutes' AS entry_time,
    breakout_type,
    entry_option_type,
    next_entry_round AS entry_round
FROM ranked_next_breakouts
WHERE rn = 1;
------------------------------------------------------------------------------------------------------------------------------------------------------
--DROP MATERIALIZED VIEW IF EXISTS mv_reentry_base_strike_selection CASCADE;

CREATE MATERIALIZED VIEW mv_reentry_base_strike_selection AS
WITH strategy AS (
    SELECT * FROM strategy_settings WHERE strategy_name = 'default'
),
reentry_info AS (
    SELECT 
        r.trade_date,
        r.entry_time,
        r.breakout_time,
        r.breakout_type,
        r.entry_round,
        CASE 
            WHEN r.breakout_type LIKE '%bullish%' THEN 'P'
            WHEN r.breakout_type LIKE '%bearish%' THEN 'C'
        END AS entry_option_type
    FROM mv_reentry_triggered_breakouts r
),
spot_price_at_breakout AS (
    SELECT 
        r.*,
        n.open AS spot_price
    FROM reentry_info r
    JOIN public."Nifty50" n
      ON r.trade_date = n.date AND r.breakout_time = n.time
),
base_with_expiry AS (
    SELECT 
        s.trade_date,
        s.entry_time,
        s.breakout_time,
        s.breakout_type,
        s.entry_round,
        s.entry_option_type,
        s.spot_price,
        o.expiry AS expiry_date
    FROM spot_price_at_breakout s
    JOIN public."nifty_options_selected_data" o
      ON o.date = s.trade_date
     AND o.option_type = s.entry_option_type
    GROUP BY s.trade_date, s.entry_time, s.breakout_time, s.breakout_type, s.entry_round, s.entry_option_type, s.spot_price, o.expiry
),
pre_ATM_calc AS (
    SELECT *, 
           ROUND(spot_price / 50.0) * 50 AS rough_atm,
           spot_price / 50.0 AS spot_div
    FROM base_with_expiry
),
atm_calc AS (
    SELECT 
        sc.*,
        CASE 
            WHEN (sc.spot_div - FLOOR(sc.spot_div)) > 0.25 THEN CEIL(sc.spot_div) * 50
            ELSE FLOOR(sc.spot_div) * 50
        END AS atm_strike
    FROM pre_ATM_calc sc
),
base_strike_selection AS (
    SELECT 
        b.*,
        o.strike,
        o.open AS entry_price,
        s.option_entry_price_cap,
        CASE
            -- ITM1 rule: if premium <= cap and direction is correct
            WHEN (
                (b.breakout_type LIKE '%bullish%' AND o.strike > b.atm_strike) OR
                (b.breakout_type LIKE '%bearish%' AND o.strike < b.atm_strike)
            )
            AND o.open <= s.option_entry_price_cap
            THEN 1
            ELSE 2
        END AS priority,
        ABS(o.open - s.option_entry_price_cap) AS premium_diff
    FROM atm_calc b
    JOIN public."nifty_options_selected_data" o
      ON o.date = b.trade_date
     AND o.time = b.entry_time
     AND o.expiry = b.expiry_date
     AND o.option_type = b.entry_option_type
    JOIN strategy s ON TRUE
),
ranked_strikes AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY trade_date, expiry_date,entry_round
            ORDER BY priority, premium_diff
        ) AS rn
    FROM base_strike_selection
)
SELECT *
FROM ranked_strikes
WHERE rn = 1;
------------------------------------------------------------------------------------------------------------------------------------------------------
--DROP MATERIALIZED VIEW IF EXISTS mv_reentry_entry_and_hedge_legs CASCADE;

CREATE MATERIALIZED VIEW mv_reentry_entry_and_hedge_legs AS
WITH strategy AS (
         SELECT *
           FROM strategy_settings
          
        ), 
		entry_strike_cte AS (
         SELECT o.date AS trade_date,
            o.expiry AS expiry_date,
            s.breakout_time,
            s.entry_time,
            s.breakout_type,
            s.entry_option_type AS option_type,
            s.spot_price,
            o.strike,
            o.open AS entry_price,
			s.entry_round,
            'RE-ENTRY'::text AS leg_type,
            'SELL'::text AS transaction_type
           FROM mv_reentry_base_strike_selection s
             JOIN strategy st ON true
             JOIN nifty_options_selected_data o ON o.date = s.trade_date AND o.expiry = s.expiry_date AND o.time::interval = s.entry_time AND o.option_type::text = s.entry_option_type AND (s.entry_option_type = 'P'::text AND o.strike >= s.strike OR s.entry_option_type = 'C'::text AND o.strike <= s.strike)
          WHERE abs(o.strike - s.strike) <= (50 * (st.num_entry_legs - 1))::numeric
        ), 
		hedge_base_strike_selection AS (
         SELECT b.trade_date,
            b.breakout_time,
            b.entry_time,
            b.breakout_type,
            b.expiry_date,
                CASE
                    WHEN b.entry_option_type = 'C'::text THEN 'P'::text
                    WHEN b.entry_option_type = 'P'::text THEN 'C'::text
                    ELSE NULL::text
                END AS hedge_option_type,
            b.spot_price,
            b.entry_round,
            o.strike,
            o.open AS hedge_price,
            row_number() OVER (PARTITION BY b.trade_date, b.expiry_date,b.entry_round ORDER BY (abs(o.open - s.hedge_entry_price_cap))) AS rn
           FROM mv_reentry_base_strike_selection b
             JOIN nifty_options_selected_data o ON o.date = b.trade_date AND o.time::interval = b.entry_time AND o.expiry = b.expiry_date AND (b.entry_option_type = 'C'::text AND o.option_type = 'P'::char OR b.entry_option_type = 'P'::text AND o.option_type = 'C'::char)
             JOIN strategy s ON true
        ), 
		selected_hedge_base_strike AS (
         SELECT hedge_base_strike_selection.trade_date,
            hedge_base_strike_selection.breakout_time,
            hedge_base_strike_selection.entry_time,
            hedge_base_strike_selection.breakout_type,
            hedge_base_strike_selection.expiry_date,
            hedge_base_strike_selection.hedge_option_type,
            hedge_base_strike_selection.spot_price,
            hedge_base_strike_selection.entry_round,
            hedge_base_strike_selection.strike,
            hedge_base_strike_selection.hedge_price,
            hedge_base_strike_selection.rn
           FROM hedge_base_strike_selection
          WHERE hedge_base_strike_selection.rn = 1
        ), hedge_strike_cte AS (
         SELECT o.date AS trade_date,
            o.expiry AS expiry_date,
            s.breakout_time,
            s.entry_time,
            s.breakout_type,
            s.hedge_option_type,
            s.spot_price,
            o.strike,
            o.open AS entry_price,
            'HEDGE-REENTRY'::text AS leg_type,
            'SELL'::text AS transaction_type,
            s.entry_round
           FROM selected_hedge_base_strike s
             JOIN strategy st ON true
             JOIN nifty_options_selected_data o ON o.date = s.trade_date AND o.expiry = s.expiry_date AND o.time::interval = s.entry_time AND o.option_type::text = s.hedge_option_type AND (s.hedge_option_type = 'P'::text AND o.strike >= s.strike OR s.hedge_option_type = 'C'::text AND o.strike <= s.strike)
          WHERE abs(o.strike - s.strike) <= (50 * (st.num_hedge_legs - 1))::numeric
        )
 SELECT entry_strike_cte.trade_date,
    entry_strike_cte.expiry_date,
    entry_strike_cte.breakout_time,
    entry_strike_cte.entry_time,
    entry_strike_cte.breakout_type,
    entry_strike_cte.option_type,
    entry_strike_cte.spot_price,
    entry_strike_cte.strike,
    entry_strike_cte.entry_price,
	entry_strike_cte.entry_round,
    entry_strike_cte.leg_type,
    entry_strike_cte.transaction_type
   FROM entry_strike_cte
UNION ALL
 SELECT hedge_strike_cte.trade_date,
    hedge_strike_cte.expiry_date,
    hedge_strike_cte.breakout_time,
    hedge_strike_cte.entry_time,
    hedge_strike_cte.breakout_type,
    hedge_strike_cte.hedge_option_type AS option_type,
    hedge_strike_cte.spot_price,
    hedge_strike_cte.strike,
    hedge_strike_cte.entry_price,
	hedge_strike_cte.entry_round,
    hedge_strike_cte.leg_type,
    hedge_strike_cte.transaction_type
   FROM hedge_strike_cte
  ORDER BY 1, 2, 10, 8, 12;
  
------------------------------------------------------------------------------------------------------------------------------------------------------  
CREATE MATERIALIZED VIEW mv_all_legs_pnl_entry_round_REENTRY AS

WITH strategy AS (
    SELECT         
		sl_type,preferred_breakout_type,
		ROUND(breakout_threshold_pct/100,3) AS breakout_threshold_pct,
        ROUND(sl_percentage/100,3) AS sl_percentage,
        ROUND(box_sl_trigger_pct/100,3) AS box_sl_trigger_pct,
        ROUND(box_sl_hard_pct/100,3) AS box_sl_hard_pct,
		ROUND(hedge_exit_entry_ratio/100,3) AS hedge_exit_entry_ratio,
		hedge_exit_multiplier,
		ROUND(leg_profit_pct/100,3) AS leg_profit_pct,
        eod_time,
        no_of_lots,lot_size
    FROM strategy_settings
),
legs AS (
    SELECT 
    e.*,
	CASE 
		WHEN e.leg_type = 'RE-ENTRY' THEN ROUND(e.entry_price * (1 + s.sl_percentage), 2) 
		ELSE 0
	END	AS sl_level
	FROM mv_reentry_entry_and_hedge_legs e
	CROSS JOIN strategy s 
	-- WHERE e.leg_type = 'ENTRY'
),
nifty_range AS (
    SELECT *
				FROM (
				    SELECT 
				        trade_date,
				        candle_time,
				        ha_high AS breakout_high,
				        ha_low AS breakout_low,
				        ROW_NUMBER() OVER (PARTITION BY trade_date ORDER BY candle_time) AS rn
				    FROM mv_ha_big_candle
				) ranked
				WHERE rn = 1  -- assumed to contain 9:15 breakout range
),
live_prices AS (
    SELECT 
        l.*,
        o.time AS ltp_time,
        o.high AS option_high,
		o.open AS option_open,
		o.close AS option_close,
        n.high AS nifty_high,
        n.low AS nifty_low,
        n.time AS nifty_time
    FROM legs l
    JOIN strategy c ON TRUE
    JOIN public.nifty_options_selected_data o
      ON o.date = l.trade_date AND o.expiry = l.expiry_date
     AND o.option_type = l.option_type AND o.strike = l.strike
     AND o.time > l.entry_time
	 AND o.time<=c.eod_time
    JOIN public."Nifty50" n
      ON n.date = l.trade_date AND n.time = o.time
    JOIN nifty_range b
      ON b.trade_date = l.trade_date
),
sl_level_calc AS(
 SELECT DISTINCT ON (trade_date, expiry_date, option_type, strike, entry_round)
        lp.trade_date,
        expiry_date,
        breakout_time,
        entry_time,
        spot_price,
        option_type,
        strike,
        entry_price,
		CASE 
        	WHEN c.sl_type = 'regular_system_sl' THEN ROUND(entry_price * (1 + c.sl_percentage), 2)
        	WHEN c.sl_type = 'box_with_buffer_sl' THEN ROUND(entry_price * (1 + c.box_sl_hard_pct), 2)
        END AS sl_level,
		entry_round,
        leg_type,
        transaction_type       
    FROM live_prices lp
	JOIN nifty_range nr ON lp.trade_date=nr.trade_date
    JOIN strategy c ON TRUE
    WHERE leg_type='RE-ENTRY' 
    ORDER BY trade_date, expiry_date, option_type, strike, entry_round, ltp_time
),
sl_triggered AS (
    SELECT DISTINCT ON (trade_date, expiry_date, option_type, strike, entry_round)
                lp.trade_date,
        lp.expiry_date,
        lp.breakout_time,
        lp.entry_time,
        lp.spot_price,
        lp.option_type,
        lp.strike,
        lp.entry_price,
		slc.sl_level,
		lp.entry_round,
        lp.leg_type,
        lp.transaction_type,       
        lp.ltp_time AS exit_time,
        slc.sl_level AS exit_price,
        'SL Hit' AS exit_reason,
        ROUND((lp.entry_price - slc.sl_level) * lot_size * c.no_of_lots, 2) AS pnl_amount
	FROM sl_level_calc slc
	JOIN  live_prices lp ON lp.trade_date=slc.trade_date AND lp.entry_time=slc.entry_time
	AND lp.strike=slc.strike
	JOIN nifty_range nr ON lp.trade_date=nr.trade_date
    JOIN strategy c ON TRUE
    WHERE slc.leg_type='RE-ENTRY' AND
         (
            -- Flat SL Logic
			lp.leg_type='RE-ENTRY' AND
            c.sl_type = 'regular_system_sl' AND lp.option_high >= ROUND(lp.entry_price * (1 + c.sl_percentage), 2)
        )
        OR (
            -- Box SL Logic: if hard SL breached
			lp.leg_type='RE-ENTRY' AND
            c.sl_type = 'box_with_buffer_sl' AND lp.option_high >= ROUND(lp.entry_price * (1 + c.box_sl_hard_pct), 2)
        )
        OR (
            -- Box SL Logic: above 20% but Nifty breaks the box
			lp.leg_type='RE-ENTRY' AND
            c.sl_type = 'box_with_buffer_sl'
            AND lp.option_high >= ROUND(lp.entry_price * (1 + c.box_sl_trigger_pct), 2)
            AND (        -- full_candle_breakout logic
        (c.preferred_breakout_type = 'full_candle_breakout'
         AND (
             (lp.option_type = 'P' AND lp.nifty_high < nr.breakout_high)
             OR
             (lp.option_type = 'C' AND lp.nifty_low > nr.breakout_low)
         )
        )
         OR

        -- pct_based logic
        (c.preferred_breakout_type = 'pct_based_breakout'
         AND (
             (lp.option_type = 'P' AND lp.nifty_high < nr.breakout_high * (1 - c.breakout_threshold_pct))
             OR
             (lp.option_type = 'C' AND lp.nifty_low > nr.breakout_low * (1 + c.breakout_threshold_pct))
         )
        )))
    ORDER BY trade_date, expiry_date, option_type, strike, entry_round, ltp_time
),
sl_leg_counts AS (
    SELECT 
        l.trade_date,
        l.expiry_date,
        l.entry_round,
        COUNT(*) FILTER (WHERE l.leg_type = 'RE-ENTRY') AS total_entry_legs,
        COUNT(*) FILTER (WHERE sl.leg_type = 'RE-ENTRY' AND sl.exit_reason = 'SL Hit') AS sl_hit_legs,
        SUM(lp.option_open) FILTER (WHERE l.leg_type = 'RE-ENTRY') AS total_entry_ltp,
        MAX(lp.option_open) FILTER (WHERE l.leg_type = 'HEDGE-REENTRY') AS hedge_ltp
    FROM legs l 
    LEFT JOIN sl_triggered sl 
        ON sl.trade_date = l.trade_date 
        AND sl.expiry_date = l.expiry_date
        AND sl.option_type = l.option_type 
        AND sl.strike = l.strike 
        AND l.entry_round = sl.entry_round
    LEFT JOIN live_prices lp 
        ON lp.trade_date = l.trade_date 
        AND lp.expiry_date = l.expiry_date
        AND lp.option_type = l.option_type 
        AND lp.strike = l.strike 
        AND lp.entry_round = l.entry_round
    GROUP BY l.trade_date, l.expiry_date, l.entry_round
),
hedge_exit_on_ALL_SL AS (
    SELECT 
        h.trade_date,
		h.expiry_date,
        h.breakout_time,
		h.entry_time,
		h.spot_price,
        h.option_type,
        h.strike,
        h.entry_price,
		0 AS sl_level,
		h.entry_round,
        'HEDGE-REENTRY' AS leg_type,
        'SELL' AS transaction_type,
        MAX(e.exit_time) AS exit_time,
		h.option_high AS exit_price,
        'ALL ENTRY SL' AS exit_reason,
        ROUND((h.entry_price - h.option_high) * c.no_of_lots * c.lot_size, 2) AS pnl_amount
    FROM live_prices h
    JOIN sl_leg_counts d ON h.trade_date = d.trade_date AND h.entry_round=d.entry_round
	AND h.expiry_date=d.expiry_date
    JOIN sl_triggered e 
	CROSS JOIN strategy c
      ON e.trade_date = h.trade_date AND e.expiry_date=h.expiry_date
	  AND e.leg_type = 'RE-ENTRY' AND e.exit_reason = 'SL Hit' AND h.entry_round=e.entry_round	   
    WHERE h.leg_type = 'HEDGE-REENTRY' AND d.total_entry_legs=d.sl_hit_legs 
    GROUP BY h.trade_date,h.expiry_date, h.option_type, h.strike, h.entry_price, h.breakout_time,
	h.entry_time,h.spot_price,h.entry_round,h.option_high,c.no_of_lots,c.lot_size,h.ltp_time
	HAVING MAX(e.exit_time) = h.ltp_time
),
rehedge_candidate AS (
    SELECT 
        h.trade_date,
		h.expiry_date,
		h.breakout_time,
        h.exit_time + INTERVAL '1 minute' AS entry_time,
		h.spot_price,
        CASE WHEN h.option_type = 'C' THEN 'P' ELSE 'C' END AS option_type,
        o.strike,
        o.open AS entry_price,
        ABS(o.open - h.exit_price) AS premium_diff,
        h.exit_price,
		h.entry_round,
        'REHEDGE-REENTRY' AS leg_type,
        'SELL' AS transaction_type,
        0 AS pnl_amount,
        'REENTRY ON SL' AS exit_status
    FROM hedge_exit_on_ALL_SL h
    JOIN public."nifty_options_selected_data" o
      ON o.date = h.trade_date 
	 AND o.expiry=h.expiry_date
     AND o.option_type = CASE WHEN h.option_type = 'C' THEN 'P' ELSE 'C' END
     AND o.time = (h.exit_time+ INTERVAL '1 minute')
),
ranked_rehedge AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY trade_date,expiry_date ORDER BY premium_diff) AS rn
    FROM rehedge_candidate
),
rehedge_leg AS (
    SELECT * FROM ranked_rehedge WHERE rn = 1
),
rehedge_eod_exit AS (
SELECT DISTINCT ON (h.trade_date, h.expiry_date, h.option_type, h.strike)
        h.trade_date,
		h.expiry_date,
		h.breakout_time,
        h.entry_time,
		h.spot_price,
        h.option_type::char,
        h.strike,
        h.entry_price,
		0 AS sl_level,
		h.entry_round,
		h.leg_type,
        'SELL'::TEXT AS transaction_type,
        sc.eod_time::TIME AS exit_time,
        o.open AS exit_price,
		'EOD CLOSE' AS exit_status,
        ROUND((h.entry_price - o.open) * sc.no_of_lots * sc.lot_size, 2) AS pnl_amount
    FROM rehedge_leg h
    JOIN strategy sc ON TRUE
    JOIN public."nifty_options_selected_data" o 
      ON o.date = h.trade_date 
	 AND o.expiry = h.expiry_date 
     AND o.option_type = h.option_type 
     AND o.strike = h.strike
     AND o.time::TIME = sc.eod_time::TIME
    -- WHERE h.leg_type = 'REHEDGE'
),
hedge_exit_50pct_NOSL AS (
 SELECT DISTINCT ON (h.trade_date, h.expiry_date, h.option_type, h.strike,h.entry_round) 
    sl.trade_date,
    sl.expiry_date,
    h.breakout_time,
    h.entry_time,
    h.spot_price,
    h.option_type::char,
    h.strike,
    h.entry_price,
    sc.hedge_exit_entry_ratio * sl.total_entry_ltp AS sl_level,
    h.entry_round,
    h.leg_type,
    'SELL'::TEXT AS transaction_type,
    h.ltp_time::TIME AS exit_time,
    h.option_open AS sl_hit_price,
    'EXIT - 50% ENTRY < HEDGE' AS exit_status,
    ROUND((h.entry_price - h.option_open) * sc.no_of_lots * sc.lot_size, 2) AS pnl_amount,
    ROW_NUMBER() OVER (
        PARTITION BY sl.trade_date, sl.expiry_date, h.option_type, h.strike, h.entry_round 
        ORDER BY h.ltp_time
    ) AS rn
FROM live_prices h
JOIN sl_leg_counts sl
    ON sl.trade_date = h.trade_date 
   AND sl.expiry_date = h.expiry_date 
   AND sl.entry_round = h.entry_round
CROSS JOIN strategy sc
WHERE sl.sl_hit_legs = 0
  AND sl.total_entry_ltp IS NOT NULL
  AND sl.hedge_ltp IS NOT NULL
  AND sl.hedge_ltp > sc.hedge_exit_entry_ratio * sl.total_entry_ltp
  ),
hedge_exit_3x_Min_one_SL AS (
  SELECT DISTINCT ON (h.trade_date, h.expiry_date, h.option_type, h.strike,h.entry_round)
    sl.trade_date,
    sl.expiry_date,
    h.breakout_time,
    h.entry_time,
    h.spot_price,
    h.option_type::char,
    h.strike,
    h.entry_price,
    sc.hedge_exit_multiplier * sl.total_entry_ltp AS sl_level,
    h.entry_round,
    h.leg_type,
    'SELL'::TEXT AS transaction_type,
    h.ltp_time::TIME AS exit_time,
    h.option_open AS sl_hit_price,
    'EXIT - 50% ENTRY < HEDGE' AS exit_status,
    ROUND((h.entry_price - h.option_open) * sc.no_of_lots * sc.lot_size, 2) AS pnl_amount,
    ROW_NUMBER() OVER (
        PARTITION BY sl.trade_date, sl.expiry_date, h.option_type, h.strike, h.entry_round 
        ORDER BY h.ltp_time
    ) AS rn
FROM live_prices h
JOIN sl_leg_counts sl
    ON sl.trade_date = h.trade_date 
   AND sl.expiry_date = h.expiry_date 
   AND sl.entry_round = h.entry_round
CROSS JOIN strategy sc
WHERE sl.sl_hit_legs >1 AND sl.sl_hit_legs!=sl.total_entry_legs
  AND sl.total_entry_ltp IS NOT NULL
  AND sl.hedge_ltp IS NOT NULL
  AND sl.hedge_ltp > sc.hedge_exit_multiplier * sl.total_entry_ltp),
closed_legs AS (
  SELECT * FROM sl_triggered
  UNION ALL 
  SELECT * FROM  hedge_exit_on_ALL_SL
  UNION ALL 
  SELECT * FROM  rehedge_eod_exit
  UNION ALL 
  SELECT trade_date,expiry_date,breakout_time,entry_time,spot_price,option_type,strike,entry_price,
    sl_level,entry_round,leg_type,transaction_type,exit_time,sl_hit_price,exit_status,pnl_amount
  FROM hedge_exit_50pct_NOSL WHERE rn=1
  UNION ALL
  SELECT trade_date,expiry_date,breakout_time,entry_time,spot_price,option_type,strike,entry_price,
    sl_level,entry_round,leg_type,transaction_type,exit_time,sl_hit_price,exit_status,pnl_amount
  FROM hedge_exit_3x_Min_one_SL WHERE rn=1
  ),
profit_booking_entry AS (
    SELECT DISTINCT ON (l.trade_date, l.expiry_date, l.option_type, l.strike,l.entry_round)
        l.trade_date,
        l.expiry_date,
		l.breakout_time,
        l.entry_time,
		l.spot_price,
        l.option_type::char,
        l.strike,
        l.entry_price,
        ROUND(l.entry_price * (1 + c.sl_percentage), 2) AS sl_level,
		l.entry_round,
        l.leg_type,
        l.transaction_type,		
        o.time AS exit_time,
        o.open AS exit_price,
        'Profit Booking ' AS exit_reason,
        ROUND((l.entry_price - o.open) * lot_size * c.no_of_lots, 2) AS pnl_amount
    FROM legs l
    JOIN strategy c ON TRUE
    JOIN public."nifty_options_selected_data" o
        ON o.date = l.trade_date AND o.expiry = l.expiry_date
       AND o.option_type = l.option_type AND o.strike = l.strike
    WHERE o.time > l.entry_time
      AND o.open <= ROUND(l.entry_price * (1-c.leg_profit_pct), 2)
	   AND NOT EXISTS (
        SELECT 1 FROM closed_legs s
        WHERE s.trade_date = l.trade_date
          AND s.expiry_date = l.expiry_date
          AND s.option_type = l.option_type
          AND s.strike = l.strike
          AND s.entry_round = l.entry_round
      )
    ORDER BY l.trade_date, l.expiry_date, l.option_type, l.strike,l.entry_round, o.time
),
eod_close AS (
    SELECT 
        l.trade_date,
		l.expiry_date,
		l.breakout_time,
        l.entry_time,
        l.spot_price,
        l.option_type,
        l.strike,
        l.entry_price,
		CASE 
        	WHEN c.sl_type = 'regular_system_sl' THEN ROUND(l.entry_price * (1 + c.sl_percentage), 2)
        	WHEN c.sl_type = 'box_with_buffer_sl' THEN ROUND(l.entry_price * (1 + c.box_sl_hard_pct), 2)
        END AS sl_level,
		l.entry_round,
		l.leg_type,
		l.transaction_type,		
        c.eod_time AS exit_time,
        o.open AS exit_price,
        -- ROUND(l.entry_price * (1 + c.sl_percentage), 2) AS sl_level,
        'EOD CLOSE' AS exit_reason,
        ROUND((l.entry_price - o.open) * lot_size * c.no_of_lots, 2) AS pnl_amount
    FROM legs l
    JOIN strategy c ON TRUE
    JOIN public.nifty_options_selected_data o
      ON o.date = l.trade_date AND o.expiry = l.expiry_date
     AND o.option_type = l.option_type AND o.strike = l.strike
     AND o.time::TIME = c.eod_time::TIME
    WHERE NOT EXISTS (
        SELECT 1 FROM closed_legs s
        WHERE s.trade_date = l.trade_date
          AND s.expiry_date = l.expiry_date
          AND s.option_type = l.option_type
          AND s.strike = l.strike
          AND s.entry_round = l.entry_round
    )
	AND NOT EXISTS (
        SELECT 1 FROM profit_booking_entry s
        WHERE s.trade_date = l.trade_date
          AND s.expiry_date = l.expiry_date
          AND s.option_type = l.option_type
          AND s.strike = l.strike
          AND s.entry_round = l.entry_round
      )
),
double_buy_legs AS (
SELECT 
    s.trade_date,
    s.expiry_date,
    s.breakout_time,
    s.exit_time AS entry_time,
    s.spot_price,
    s.option_type::char,
    s.strike,
    s.exit_price AS entry_price,
    0 AS sl_level,
	 s.entry_round,
    'DOUBLE BUY-REENTRY' AS leg_type,
    'BUY' AS transaction_type,
    c.eod_time AS exit_time,
    o.open AS exit_price,
	 'EOD EXIT' AS exit_reason,
    -- CASE 
    --     WHEN n.next_entry_time IS NOT NULL THEN 'Exit for Reentry'
    --     ELSE 'EOD EXIT'
    -- END AS exit_reason,
    --ROUND((o.open - s.exit_price) * c.lot_size * c.no_of_lots, 2) AS pnl_amount
	ROUND((s.exit_price - o.open) * c.lot_size * c.no_of_lots, 2) AS pnl_amount
FROM sl_triggered s
JOIN strategy c ON TRUE
-- LEFT JOIN next_entry_time n 
--   ON s.trade_date = n.trade_date 
--  AND s.expiry_date = n.expiry_date 
--  AND s.entry_round + 1 = n.entry_round
JOIN public.nifty_options_selected_data o 
  ON o.date = s.trade_date
 AND o.expiry = s.expiry_date
 AND o.option_type = s.option_type
 AND o.strike = s.strike
 AND o.time = c.eod_time
 ORDER BY s.trade_date,s.expiry_date )
 
SELECT * FROM closed_legs
UNION ALL
SELECT * FROM profit_booking_entry
UNION ALL
SELECT * FROM eod_close
UNION ALL
SELECT * FROM double_buy_legs
ORDER BY trade_date, expiry_date, entry_time, exit_time,strike,leg_type, entry_round;

-------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE sp_run_reentry_loop(p_strategy_name TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    v_max_rounds INT;
    v_round INT;
BEGIN
    -- Step 1: Get max_entry_rounds for the given strategy
    SELECT max_reentry_rounds
    INTO v_max_rounds
    FROM strategy_settings
    WHERE strategy_name = p_strategy_name;

    IF v_max_rounds IS NULL THEN
        RAISE EXCEPTION 'No max_entry_rounds found for strategy_nmae = %', p_strategy_name;
    END IF;

    RAISE NOTICE 'Starting re-entry loop for strategy_id = % with max rounds = %', p_strategy_name, v_max_rounds;

    -- Step 2: Loop through entry rounds
    FOR v_round IN 2..v_max_rounds LOOP
        RAISE NOTICE 'Processing re-entry round %', v_round;

        -- Step 3: Refresh all required materialized views for this round
        RAISE NOTICE 'Refreshing materialized views for round %', v_round;
        REFRESH MATERIALIZED VIEW  mv_ranked_breakouts_with_rounds_for_reentry;
        -- REFRESH MATERIALIZED VIEW  mv_reentry_triggered_breakouts;
        -- REFRESH MATERIALIZED VIEW  mv_reentry_base_strike_selection;
        -- REFRESH MATERIALIZED VIEW  mv_reentry_entry_and_hedge_legs;
        -- REFRESH MATERIALIZED VIEW  mv_reentry_leg_with_sl_level;
        -- REFRESH MATERIALIZED VIEW  mv_reentry_sl_tracking_withboxstrategy;
		REFRESH MATERIALIZED VIEW mv_reentry_triggered_breakouts;
		REFRESH MATERIALIZED VIEW mv_reentry_base_strike_selection;
		REFRESH MATERIALIZED VIEW mv_reentry_entry_and_hedge_legs;
		REFRESH MATERIALIZED VIEW mv_all_legs_pnl_entry_round_REENTRY ;

        -- Step 4: Insert new legs into strategy_leg_book
        RAISE NOTICE 'Inserting legs into strategy_leg_book for round %', v_round;
 INSERT INTO strategy_leg_book (
        trade_date,
        expiry_date,
        breakout_time,
        entry_time,
        exit_time,
        option_type,
        strike,
        entry_price,
        exit_price,
        transaction_type,
        leg_type,
        entry_round,
        exit_reason
    )
    SELECT 
        trade_date,
        expiry_date,
        breakout_time,
        entry_time,
        exit_time,
        option_type,
        strike,
        entry_price,
        exit_price,
        transaction_type,
        leg_type,
        entry_round,
        exit_reason
    FROM mv_all_legs_pnl_entry_round_REENTRY sl
    WHERE NOT EXISTS (
        SELECT 1
        FROM strategy_leg_book b
        WHERE b.trade_date = sl.trade_date
          AND b.expiry_date = sl.expiry_date
          AND b.option_type = sl.option_type
          AND b.strike = sl.strike
          AND b.entry_round = sl.entry_round
          AND b.leg_type = sl.leg_type ); 
          -- AND entry_round = v_round);
    END LOOP;

    RAISE NOTICE 'Re-entry loop completed for strategy_name = %', p_strategy_name;

EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING 'Error occurred: %', SQLERRM;
        RAISE NOTICE 'Aborting re-entry loop for strategy_name = %', p_strategy_name;
END $$;
CALL sp_run_reentry_loop('default');
-------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE MATERIALIZED VIEW mv_entry_Leg_live_prices AS
WITH config AS (
    SELECT 
        sl_type,
        sl_percentage,
        box_sl_trigger_pct,
        box_sl_hard_pct,
        eod_time,
        no_of_lots
    FROM strategy_settings
),
legs AS (
SELECT * FROM mv_all_legs_pnl_entry_round_REENTRY
UNION ALL
SELECT * FROM mv_all_legs_pnl_entry_round1
-- SELECT * FROM mv_entry_sl_tracking_withboxstrategy
-- UNION ALL
-- SELECT * FROM mv_reentry_sl_tracking_withboxstrategy
-- UNION ALL
-- SELECT * FROM mv_hedge_exit_and_reentry
-- UNION ALL
-- SELECT * FROM mv_hedge_leg_exit
-- UNION ALL
-- SELECT * FROM mv_rehedge_exit_and_reentry
-- UNION ALL
-- SELECT * FROM mv_hedge_reentry_leg_exit
),
nifty_range AS (
    SELECT 
        trade_date,
        ha_high AS breakout_high,
        ha_low AS breakout_low
    FROM mv_ha_15min_candle  -- assumed to contain 9:15 breakout range
)
    SELECT 
        l.*,
        o.time AS ltp_time,
        o.high AS option_high,
		o.open AS option_open,
        n.high AS nifty_high,
        n.low AS nifty_low,
        n.time AS nifty_time
    FROM legs l
    JOIN config c ON TRUE
    JOIN public.nifty_options_selected_data o
      ON o.date = l.trade_date AND o.expiry = l.expiry_date
     AND o.option_type = l.option_type AND o.strike = l.strike
     AND o.time > l.entry_time
    JOIN public."Nifty50" n
      ON n.date = l.trade_date AND n.time = o.time
    JOIN nifty_range b
      ON b.trade_date = l.trade_date;
-------------------------------------------------------------------------------------------------------------------------------------------------------
-- SELECT * FROM mv_all_legs_pnl_entry_round_REENTRY;

--DROP MATERIALIZED VIEW IF EXISTS mv_ALL_ENTRIES_sl_tracking_adjusted ;

CREATE MATERIALIZED VIEW mv_ALL_ENTRIES_sl_tracking_adjusted AS
WITH 
config AS (
    SELECT 
        sl_type,
        sl_percentage,
        box_sl_trigger_pct,
        box_sl_hard_pct,
        eod_time,
        no_of_lots,lot_size
    FROM strategy_settings
),

next_round_reentry_times AS (
    SELECT
        trade_date,
        entry_round - 1 AS prior_round,
        -- MIN(breakout_time)::INTERVAL AS next_round_start_time
		entry_time::INTERVAL AS next_round_start_time
    FROM mv_reentry_triggered_breakouts
    -- GROUP BY trade_date, entry_round,entry_time
),
all_legs AS (
SELECT * FROM mv_all_legs_pnl_entry_round_REENTRY
UNION ALL
SELECT * FROM mv_all_legs_pnl_entry_round1
-- UNION ALL
-- SELECT * FROM mv_hedge_exit_and_reentry
-- UNION ALL
-- SELECT * FROM mv_hedge_leg_exit
-- UNION ALL
-- SELECT * FROM mv_rehedge_exit_and_reentry
-- UNION ALL
-- SELECT * FROM mv_hedge_reentry_leg_exit
-- UNION ALL
-- SELECT * FROM mv_double_buy_legs
),

adjusted_exit_time_data AS (
    SELECT
        l.*,
        CASE
            WHEN l.exit_time>r.next_round_start_time
                 AND r.next_round_start_time IS NOT NULL
                 AND l.entry_round = r.prior_round
            THEN r.next_round_start_time
            ELSE l.exit_time::INTERVAL
        END AS adjusted_exit_time,

        CASE
            WHEN l.exit_time>r.next_round_start_time
                 AND r.next_round_start_time IS NOT NULL
                 AND l.entry_round = r.prior_round
            THEN 'Closed due to re-entry'
            ELSE l.exit_reason
        END AS adjusted_exit_reason
		
    FROM all_legs l
    LEFT JOIN next_round_reentry_times r 
	ON l.trade_date = r.trade_date AND l.entry_round = r.prior_round 
),

adjusted_exit_price_data AS (
	SELECT
        l.*,
		CASE
            WHEN l.adjusted_exit_reason='Closed due to re-entry'                 
            THEN r.option_open
            ELSE l.exit_price
        END AS adjusted_exit_price
		 
    FROM adjusted_exit_time_data l
    JOIN mv_entry_Leg_live_prices r 
	ON l.trade_date = r.trade_date AND
	  r.expiry_date = l.expiry_date
       AND r.option_type = l.option_type AND r.strike = l.strike
	AND r.ltp_time=l.adjusted_exit_time
	-- AND l.exit_reason='Closed due to re-entry'
)

SELECT DISTINCT ON (l.trade_date, l.expiry_date, l.option_type, l.strike,l.leg_type,l.entry_round)
        l.trade_date,
		l.expiry_date,
		l.breakout_time,
        l.entry_time,
        l.spot_price,
        l.option_type,
        l.strike,
        l.entry_price,
		l.sl_level,
		l.entry_round,
		l.leg_type,
		l.transaction_type,		
        l.adjusted_exit_time::Time AS exit_time,
        l.adjusted_exit_price AS exit_price,
        -- ROUND(l.entry_price * (1 + c.sl_percentage), 2) AS sl_level,
        l.adjusted_exit_reason AS exit_reason,
		ROUND(
			CASE 
			WHEN l.transaction_type = 'BUY' THEN (l.adjusted_exit_price - l.entry_price)
			ELSE (l.entry_price - l.adjusted_exit_price)
			END * lot_size * c.no_of_lots, 2) AS pnl_amount
        --ROUND((l.entry_price -l.adjusted_exit_price) * lot_size * c.no_of_lots, 2) AS pnl_amount
FROM adjusted_exit_price_data l
JOIN config c ON TRUE
-- where trade_date='2025-06-26'
ORDER BY 
l.trade_date, l.expiry_date, l.option_type, l.strike,l.entry_round,l.leg_type;
-----------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE MATERIALIZED VIEW mv_portfolio_mtm_pnl AS
WITH config AS (
    SELECT 
        portfolio_capital, 
        portfolio_profit_target_pct, 
        portfolio_stop_loss_pct, 
        no_of_lots
    FROM strategy_settings
),

-- All strategy legs (entry, hedge, reentry, etc.)
all_legs AS (
		SELECT * FROM mv_ALL_ENTRIES_sl_tracking_adjusted 
    -- SELECT * FROM mv_entry_sl_tracking_withboxstrategy
    -- UNION ALL
    -- SELECT * FROM mv_hedge_leg_exit
    -- UNION ALL
    -- SELECT * FROM mv_hedge_exit_and_reentry
),

-- All minute-wise timestamps from options data (after 09:35 AM)
all_times AS (
    SELECT DISTINCT 
        o.date AS trade_date,
        o.expiry AS expiry_date,
        o.time
    FROM public.nifty_options_selected_data o
    WHERE o.time >= '09:36:00'  -- Assuming all entries are complete at this time
),

-- -- Realized PnL from closed legs where exit_time < current time
closed_pnl_at_time AS (
    SELECT 
        t.trade_date,
        t.expiry_date,
		-- l.entry_round,
		-- l.entry_price ,
		-- l.exit_price,
        t.time,
        SUM(
            CASE 
                WHEN l.transaction_type = 'SELL' THEN (l.entry_price - l.exit_price)
                ELSE (l.exit_price - l.entry_price)
            END * 75 * c.no_of_lots
        ) AS realized_pnl
    FROM all_times t
    JOIN all_legs l 
      ON l.trade_date = t.trade_date AND l.expiry_date = t.expiry_date
    JOIN config c ON TRUE
    WHERE l.exit_time IS NOT NULL AND l.exit_time < t.time 
	-- aND l.trade_date='2025-06-18'
    GROUP BY t.trade_date, t.expiry_date,
	-- l.entry_price ,		l.exit_price, 
		t.time
),

-- -- MTM PnL from open legs at that minute
open_mtm_at_time AS (
    SELECT 
        t.trade_date,
        t.expiry_date,
		-- l.entry_round,
        t.time,
        SUM(
            CASE 
                WHEN l.transaction_type = 'SELL' THEN (l.entry_price - o.open)
                ELSE (o.open - l.entry_price)
            END * 75 * c.no_of_lots
        ) AS unrealized_pnl
    FROM all_times t
    JOIN all_legs l 
      ON l.trade_date = t.trade_date AND l.expiry_date = t.expiry_date
    JOIN config c ON TRUE
    JOIN public.nifty_options_selected_data o 
      ON o.date = l.trade_date AND o.expiry = l.expiry_date
     AND o.option_type = l.option_type AND o.strike = l.strike AND o.time = t.time
    WHERE l.entry_time <= t.time AND (l.exit_time IS NULL OR t.time < l.exit_time) 
	-- aND l.trade_date='2025-06-18'
    GROUP BY t.trade_date, t.expiry_date, t.time
),

-- -- Final combined PnL (realized + unrealized)
portfolio_mtm_pnl AS (
    SELECT 
        t.trade_date,
        t.expiry_date,
        t.time,
		-- o.entry_round,
        ROUND(COALESCE(c.realized_pnl, 0) + COALESCE(o.unrealized_pnl, 0), 2) AS total_pnl,
        ROUND(COALESCE(c.realized_pnl, 0), 2) AS realized_pnl,
        ROUND(COALESCE(o.unrealized_pnl, 0), 2) AS unrealized_pnl
    FROM all_times t
    LEFT JOIN closed_pnl_at_time c 
      ON t.trade_date = c.trade_date AND t.expiry_date = c.expiry_date AND t.time = c.time
    LEFT JOIN open_mtm_at_time o 
      ON t.trade_date = o.trade_date AND t.expiry_date = o.expiry_date AND t.time = o.time 
	  -- WHERE  t.trade_date='2025-06-18'
	  ORDER BY t.time
)

-- Final Output
SELECT * 
FROM portfolio_mtm_pnl
-- WHERE trade_date='2025-06-19'
ORDER BY trade_date, expiry_date, time;

---------------------------------------------------------------------------------------------------------------------------------------------------------
-- DROP MATERIALIZED VIEW IF EXISTS mv_portfolio_final_pnl cascade;
CREATE MATERIALIZED VIEW mv_portfolio_final_pnl AS

WITH config AS (
    SELECT 
        portfolio_capital, 
        ROUND(portfolio_profit_target_pct/100,3) AS portfolio_profit_target_pct, 
        ROUND(portfolio_stop_loss_pct/100,3) AS  portfolio_stop_loss_pct,
        no_of_lots,lot_size,eod_time
    FROM strategy_settings
),

-- Use your new minute-wise portfolio PnL (from previous query)
portfolio_mtm_pnl AS (
    -- assume this is the same CTE or materialized view you've created
    SELECT * FROM mv_portfolio_mtm_pnl -- or replace with inline query if not yet materialized
),

-- -- -- Step 1: Detect first minute when portfolio hits SL or Target
portfolio_exit_trigger AS (
    SELECT DISTINCT ON (trade_date, expiry_date)
        trade_date,
        expiry_date,
        time AS exit_time,
        total_pnl,
        CASE 
            WHEN total_pnl >= c.portfolio_capital * c.portfolio_profit_target_pct THEN 'Portfolio Exit - Profit'
            WHEN total_pnl <= -c.portfolio_capital * c.portfolio_stop_loss_pct THEN 'Portfolio Exit - Loss'
        END AS exit_reason
    FROM portfolio_mtm_pnl
    JOIN config c ON TRUE
    WHERE 
        total_pnl >= c.portfolio_capital * c.portfolio_profit_target_pct
        OR total_pnl <= -c.portfolio_capital * c.portfolio_stop_loss_pct
    ORDER BY trade_date, expiry_date, time
),

-- Step 2: Get all legs that are still open at the time of exit
open_legs_at_exit AS (
    SELECT 
        l.*
    FROM (
	SELECT * FROM mv_ALL_ENTRIES_sl_tracking_adjusted
        -- SELECT * FROM mv_entry_sl_tracking_withboxstrategy
        -- UNION ALL
        -- SELECT * FROM mv_hedge_leg_exit
        -- UNION ALL
        -- SELECT * FROM mv_hedge_exit_and_reentry
    ) l
    JOIN portfolio_exit_trigger p 
      ON l.trade_date = p.trade_date AND l.expiry_date = p.expiry_date
    WHERE l.exit_time IS NULL OR p.exit_time <= l.exit_time AND p.exit_time>=l.entry_time 
),

-- Step 3: Attach LTP and mark exit at the exit_time
exit_priced_legs AS (
    SELECT 
        l.trade_date,
        l.expiry_date,
        l.breakout_time,
        l.entry_time,
        l.spot_price,
        l.option_type,
        l.strike,
        l.entry_price,
        l.sl_level,
		l.entry_round,
        l.leg_type,
        l.transaction_type,
        p.exit_time,
		o.open AS exit_price,
        p.exit_reason

    FROM open_legs_at_exit l
    JOIN portfolio_exit_trigger p 
      ON l.trade_date = p.trade_date AND l.expiry_date = p.expiry_date
    JOIN public.nifty_options_selected_data o
      ON o.date = l.trade_date 
     AND o.expiry = l.expiry_date 
     AND o.option_type = l.option_type 
     AND o.strike = l.strike 
     AND o.time = p.exit_time
),

-- Step 4: Calculate exit PnL
exit_legs_with_pnl AS (
    SELECT 
        e.*,
        CASE 
            WHEN e.transaction_type = 'SELL' THEN ROUND((e.entry_price - e.exit_price) * lot_size * c.no_of_lots, 2)
            ELSE ROUND((e.exit_price - e.entry_price) * lot_size * c.no_of_lots, 2)
        END AS pnl_amount
    FROM exit_priced_legs e
    CROSS JOIN config c
),
invalid_legs_with_exit AS (
	SELECT l.* FROM mv_ALL_ENTRIES_sl_tracking_adjusted  l
	    WHERE l.entry_time>l.exit_time),
		
invalid_legs_with_pnl AS (
	SELECT l.* FROM mv_ALL_ENTRIES_sl_tracking_adjusted  l
	JOIN portfolio_exit_trigger p 
      ON l.trade_date = p.trade_date AND l.expiry_date = p.expiry_date
    WHERE l.entry_time>p.exit_time
	
),
valid_legs_with_pnl AS (
	SELECT * FROM mv_ALL_ENTRIES_sl_tracking_adjusted  h
	WHERE NOT EXISTS (
          SELECT 1 
          FROM invalid_legs_with_pnl hex
          WHERE hex.trade_date = h.trade_date
            AND hex.option_type = h.option_type
            AND hex.strike = h.strike
			aND hex.entry_round=h.entry_round
			AND hex.leg_type=h.leg_type
      ) AND 
	  NOT EXISTS (
          SELECT 1 
          FROM invalid_legs_with_exit hex
          WHERE hex.trade_date = h.trade_date
            AND hex.option_type = h.option_type
            AND hex.strike = h.strike
			AND hex.entry_round=h.entry_round
			AND hex.leg_type=h.leg_type
      )
),
all_leg_exits AS (
	SELECT * FROM valid_legs_with_pnl
    UNION ALL
    SELECT * FROM exit_legs_with_pnl
),
reentry_exit_summary AS (
	SELECT
		trade_date,
		expiry_date,
		MAX(exit_time) AS max_exit_time,
		COUNT(*) FILTER (WHERE exit_time IS NOT NULL) AS exited_count,
		COUNT(*) AS total_reentry_legs
	FROM all_leg_exits
	WHERE leg_type = 'RE-ENTRY'
	GROUP BY trade_date, expiry_date
),

hedge_exit_on_reentry_completion AS (
	SELECT
		h.trade_date,
		h.expiry_date,
		h.breakout_time	,
		h.entry_time	,
		h.spot_price	,
		h.option_type,
		h.strike,
		h.entry_price,
		0 as sl_level,
		h.entry_round,
		h.leg_type	,
		h.transaction_type	,
		r.max_exit_time AS exit_time,
		o.open as exit_price,		
		'EXIT - ALL REENTRY COMPLETE' AS exit_reason,
		 CASE 
            WHEN h.transaction_type = 'SELL' THEN ROUND((h.entry_price - o.open) * lot_size * c.no_of_lots, 2)
            ELSE ROUND((o.open - h.entry_price) * lot_size * c.no_of_lots, 2)
        END AS pnl_amount
	FROM all_leg_exits h
	JOIN config c ON TRUE
	 JOIN public.nifty_options_selected_data o
      ON o.date = h.trade_date 
     AND o.expiry = h.expiry_date 
     AND o.option_type = h.option_type 
     AND o.strike = h.strike 
     -- AND o.time = r.max_exit_time
	JOIN reentry_exit_summary r
		ON h.trade_date = r.trade_date
		AND h.expiry_date = r.expiry_date
		AND o.time = r.max_exit_time
	WHERE h.leg_type = 'HEDGE-REENTRY'
	  AND r.exited_count = r.total_reentry_legs
	  AND r.max_exit_time !=c.eod_time
),
all_leg_exits_new AS (
	SELECT * FROM valid_legs_with_pnl
    UNION ALL
    SELECT * FROM exit_legs_with_pnl
	UNION ALL
	SELECT * FROM hedge_exit_on_reentry_completion
),

ranked_legs AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY 
                   trade_date,
                   expiry_date,
                   option_type,
                   strike,
                   leg_type , -- to distinguish ENTRY vs HEDGE
				   entry_round
               ORDER BY exit_time
           ) AS rn
    FROM all_leg_exits_new
)

SELECT trade_date	,
expiry_date	,
breakout_time	,
entry_time	,
spot_price	,
option_type	,
strike	,
entry_price	,
sl_level	,
entry_round,
leg_type	,
transaction_type	,
exit_time	,
exit_price	,
exit_reason	,
pnl_amount	,
ROUND(SUM(pnl_amount) OVER (PARTITION BY trade_date, expiry_date), 2) AS total_pnl_per_day
 
FROM ranked_legs
WHERE rn = 1
ORDER BY trade_date, expiry_date, entry_time, option_type, strike;

----------------------------------------------------------------------------------------------------------------------------------------------------------
