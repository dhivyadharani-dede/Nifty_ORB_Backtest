DROP TABLE IF EXISTS strategy_settings CASCADE;
CREATE TABLE strategy_settings (
	--id BOOLEAN PRIMARY KEY DEFAULT TRUE,
	strategy_name TEXT PRIMARY KEY DEFAULT 'default',
	big_candle_tf NUMERIC DEFAULT 15,
	small_candle_tf NUMERIC DEFAULT 5,
    preferred_breakout_type TEXT 
		CHECK (preferred_breakout_type IN ('full_candle_breakout', 'pct_based_breakout')) DEFAULT 'full_candle_breakout',
	reentry_breakout_type TEXT 
		CHECK (preferred_breakout_type IN ('full_candle_breakout', 'pct_based_breakout')) DEFAULT 'full_candle_breakout',
    breakout_threshold_pct NUMERIC DEFAULT 60,
    option_entry_price_cap NUMERIC DEFAULT 80,
    hedge_entry_price_cap NUMERIC DEFAULT 50,
    num_entry_legs INTEGER DEFAULT 4,
    num_hedge_legs INTEGER DEFAULT 1,
	sl_percentage NUMERIC DEFAULT 20,-- user input should be in percentage
	eod_time TIME DEFAULT '15:20:00',
	no_of_lots INTEGER DEFAULT 1,
	lot_size INTEGER DEFAULT 75,
	hedge_exit_entry_ratio NUMERIC DEFAULT 50,--ALL SL open, all legs exit
	hedge_exit_multiplier NUMERIC DEFAULT 3, --3X all leg exit
	leg_profit_pct NUMERIC DEFAULT 84,--full set up exit
	portfolio_profit_target_pct NUMERIC DEFAULT 2,--user inout should in percentage 
	portfolio_stop_loss_pct NUMERIC DEFAULT 2,-- user input should be in percentage
	portfolio_capital NUMERIC DEFAULT 900000,
	max_reentry_rounds NUMERIC DEFAULT 1,--should will
	sl_type TEXT CHECK (sl_type IN ('regular_system_sl', 'box_with_buffer_sl')) DEFAULT 'regular_system_sl',
	box_sl_trigger_pct NUMERIC DEFAULT 2,
	box_sl_hard_pct NUMERIC DEFAULT 2
	
);

	-- Insert default config (if not already present)
INSERT INTO strategy_settings (
	big_candle_tf,
	small_candle_tf,
    preferred_breakout_type, 
	reentry_breakout_type,
	breakout_threshold_pct,
    option_entry_price_cap, 
	hedge_entry_price_cap,
    num_entry_legs, 
	num_hedge_legs,
	sl_percentage ,
	eod_time,
	no_of_lots,
	lot_size,
	hedge_exit_entry_ratio,
	hedge_exit_multiplier,
	leg_profit_pct,
	portfolio_profit_target_pct,
	portfolio_stop_loss_pct,
	portfolio_capital,
	max_reentry_rounds,
	sl_type,
	box_sl_trigger_pct,
	box_sl_hard_pct	
) 
VALUES (15,5,'full_candle_breakout','full_candle_breakout', 60, 80, 50, 4, 1,20,'15:20:00',1,75,50,3,85,2,2,900000,1,'regular_system_sl',20,50)
ON CONFLICT (strategy_name) DO UPDATE
SET preferred_breakout_type = EXCLUDED.preferred_breakout_type,
	reentry_breakout_type = EXCLUDED.reentry_breakout_type,
    breakout_threshold_pct = EXCLUDED.breakout_threshold_pct,
	option_entry_price_cap=EXCLUDED.option_entry_price_cap,
	hedge_entry_price_cap=EXCLUDED.hedge_entry_price_cap,
    num_entry_legs=EXCLUDED.num_entry_legs,
	num_hedge_legs=EXCLUDED.num_hedge_legs,
	sl_percentage=EXCLUDED.sl_percentage,
	eod_time=EXCLUDED.eod_time,
	no_of_lots=EXCLUDED.no_of_lots,
	lot_size=EXCLUDED.lot_size,
	hedge_exit_entry_ratio=EXCLUDED.hedge_exit_entry_ratio,
	hedge_exit_multiplier=EXCLUDED.hedge_exit_multiplier,
	leg_profit_pct=EXCLUDED.leg_profit_pct,
	portfolio_profit_target_pct=EXCLUDED.portfolio_profit_target_pct,
	portfolio_stop_loss_pct=EXCLUDED.portfolio_stop_loss_pct,
	portfolio_capital=EXCLUDED.portfolio_capital,	
	max_reentry_rounds=EXCLUDED.max_reentry_rounds,
	sl_type=EXCLUDED.sl_type,
	box_sl_trigger_pct=EXCLUDED.box_sl_trigger_pct,
	box_sl_hard_pct=EXCLUDED.box_sl_hard_pct
	;
	
Select * from strategy_settings;
-------------------------------------------------------------------------------------------------------------------------------------------------------


UPDATE strategy_settings
SET 
	big_candle_tf=15,
	small_candle_tf=5,
	preferred_breakout_type = 'full_candle_breakout',
	reentry_breakout_type = 'full_candle_breakout',
    breakout_threshold_pct = 100
	option_entry_price_cap =80,
    hedge_entry_price_cap = 50,
    num_entry_legs = 4,
    num_hedge_legs = 1,
	sl_percentage = 20,
	eod_time TIME =T '15:20:00',
	no_of_lots = 1,
	lot_size=75,
	hedge_exit_entry_ratio = 50,
	hedge_exit_multiplier = 3,
	leg_profit_pct = 85,
	portfolio_profit_target_pct = 2,
	portfolio_stop_loss_pct = 2,
	portfolio_capital = 900000,
	max_reentry_rounds = 1,
	sl_type = 'flat',
	box_sl_trigger_pct = 20,
	box_sl_hard_pct= 50
WHERE strategy_name = 'default';