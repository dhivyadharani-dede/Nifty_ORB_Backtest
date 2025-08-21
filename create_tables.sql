-- Table: public.Nifty50

--DROP TABLE IF EXISTS public."Nifty50";

CREATE TABLE IF NOT EXISTS public."Nifty50"
(
    date date,
    "time" time without time zone,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    volume numeric,
    oi numeric,
    option_nm "char"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."Nifty50"
    OWNER to postgres;
-- Index: idx_nifty50_date_time

-- DROP INDEX IF EXISTS public.idx_nifty50_date_time;

CREATE INDEX IF NOT EXISTS idx_nifty50_date_time
    ON public."Nifty50" USING btree
    (date ASC NULLS LAST, "time" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_nifty50_datetime

-- DROP INDEX IF EXISTS public.idx_nifty50_datetime;

CREATE INDEX IF NOT EXISTS idx_nifty50_datetime
    ON public."Nifty50" USING btree
    (date ASC NULLS LAST, "time" ASC NULLS LAST)
    TABLESPACE pg_default;
	
-- Table: public.Nifty_options

-- DROP TABLE IF EXISTS public."Nifty_options";

CREATE TABLE IF NOT EXISTS public."Nifty_options"
(
    symbol "char",
    date date,
    expiry date,
    strike numeric,
    option_type "char",
    "time" time without time zone,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    volume numeric,
    oi numeric,
    option_nm "char"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."Nifty_options"
    OWNER to postgres;
-- Index: idx_nifty_options_lookup

-- DROP INDEX IF EXISTS public.idx_nifty_options_lookup;

CREATE INDEX IF NOT EXISTS idx_nifty_options_lookup
    ON public."Nifty_options" USING btree
    (date ASC NULLS LAST, option_type ASC NULLS LAST, strike ASC NULLS LAST, "time" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_opt_basic

-- DROP INDEX IF EXISTS public.idx_opt_basic;

CREATE INDEX IF NOT EXISTS idx_opt_basic
    ON public."Nifty_options" USING btree
    (date ASC NULLS LAST, option_type ASC NULLS LAST, strike ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_opt_eod_time

-- DROP INDEX IF EXISTS public.idx_opt_eod_time;

CREATE INDEX IF NOT EXISTS idx_opt_eod_time
    ON public."Nifty_options" USING btree
    (date ASC NULLS LAST, expiry ASC NULLS LAST, "time" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_opt_ltp_tracking

-- DROP INDEX IF EXISTS public.idx_opt_ltp_tracking;

CREATE INDEX IF NOT EXISTS idx_opt_ltp_tracking
    ON public."Nifty_options" USING btree
    (date ASC NULLS LAST, expiry ASC NULLS LAST, option_type ASC NULLS LAST, strike ASC NULLS LAST, "time" ASC NULLS LAST)
    TABLESPACE pg_default;
	
-- Table: public.nifty_options_selected_data

-- DROP TABLE IF EXISTS public.nifty_options_selected_data;

CREATE TABLE IF NOT EXISTS public.nifty_options_selected_data
(
    symbol "char",
    date date,
    expiry date,
    strike numeric,
    option_type "char",
    "time" time without time zone,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    volume numeric,
    oi numeric,
    option_nm "char"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.nifty_options_selected_data
    OWNER to postgres;
	
-- Table: public.strategy_settings

-- DROP TABLE IF EXISTS public.strategy_settings;

CREATE TABLE IF NOT EXISTS public.strategy_settings
(
    strategy_name text COLLATE pg_catalog."default" NOT NULL DEFAULT 'default'::text,
    big_candle_tf numeric DEFAULT 15,
    small_candle_tf numeric DEFAULT 5,
    preferred_breakout_type text COLLATE pg_catalog."default" DEFAULT 'full_candle_breakout'::text,
    breakout_threshold_pct numeric DEFAULT 60,
    option_entry_price_cap numeric DEFAULT 80,
    hedge_entry_price_cap numeric DEFAULT 50,
    num_entry_legs integer DEFAULT 4,
    num_hedge_legs integer DEFAULT 1,
    sl_percentage numeric DEFAULT 20,
    eod_time time without time zone DEFAULT '15:20:00'::time without time zone,
    no_of_lots integer DEFAULT 1,
    lot_size integer DEFAULT 75,
    hedge_exit_entry_ratio numeric DEFAULT 50,
    hedge_exit_multiplier numeric DEFAULT 3,
    leg_profit_pct numeric DEFAULT 84,
    portfolio_profit_target_pct numeric DEFAULT 2,
    portfolio_stop_loss_pct numeric DEFAULT 2,
    portfolio_capital numeric DEFAULT 900000,
    max_reentry_rounds numeric DEFAULT 1,
    sl_type text COLLATE pg_catalog."default" DEFAULT 'regular_system_sl'::text,
    box_sl_trigger_pct numeric DEFAULT 2,
    box_sl_hard_pct numeric DEFAULT 2,
    reentry_breakout_type text COLLATE pg_catalog."default" DEFAULT 'full_candle_breakout'::text,
    CONSTRAINT strategy_settings_pkey PRIMARY KEY (strategy_name),
    CONSTRAINT strategy_settings_preferred_breakout_type_check CHECK (preferred_breakout_type = ANY (ARRAY['full_candle_breakout'::text, 'pct_based_breakout'::text])),
    CONSTRAINT strategy_settings_sl_type_check CHECK (sl_type = ANY (ARRAY['regular_system_sl'::text, 'box_with_buffer_sl'::text])),
    CONSTRAINT strategy_settings_preferred_breakout_type_check1 CHECK (preferred_breakout_type = ANY (ARRAY['full_candle_breakout'::text, 'pct_based_breakout'::text]))
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.strategy_settings
    OWNER to postgres;

----------------------------------------------------------------------------------------------------------------------------------------------------------
-- Table: public.strategy_leg_book

-- DROP TABLE IF EXISTS public.strategy_leg_book;

CREATE TABLE IF NOT EXISTS public.strategy_leg_book
(
    trade_date date NOT NULL,
    expiry_date date NOT NULL,
    breakout_time time without time zone,
    entry_time time without time zone NOT NULL,
    exit_time time without time zone,
    option_type text COLLATE pg_catalog."default" NOT NULL,
    strike numeric NOT NULL,
    entry_price numeric NOT NULL,
    exit_price numeric,
    transaction_type text COLLATE pg_catalog."default" NOT NULL,
    leg_type text COLLATE pg_catalog."default" NOT NULL,
    entry_round integer NOT NULL DEFAULT 1,
    exit_reason text COLLATE pg_catalog."default",
    CONSTRAINT strategy_leg_book_pkey PRIMARY KEY (trade_date, expiry_date, strike, option_type, entry_round, leg_type),
    CONSTRAINT strategy_leg_book_option_type_check CHECK (option_type = ANY (ARRAY['C'::text, 'P'::text])),
    CONSTRAINT strategy_leg_book_transaction_type_check CHECK (transaction_type = ANY (ARRAY['BUY'::text, 'SELL'::text])),
    CONSTRAINT strategy_leg_book_leg_type_check CHECK (leg_type = ANY (ARRAY['ENTRY'::text, 'HEDGE'::text, 'DOUBLE BUY'::text, 'RE-ENTRY'::text, 'REHEDGE'::text, 'HEDGE-REENTRY'::text]))
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.strategy_leg_book
    OWNER to postgres;
-- Index: idx_legbook_rounds

-- DROP INDEX IF EXISTS public.idx_legbook_rounds;

CREATE INDEX IF NOT EXISTS idx_legbook_rounds
    ON public.strategy_leg_book USING btree
    (trade_date ASC NULLS LAST, entry_round ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_legbook_status

-- DROP INDEX IF EXISTS public.idx_legbook_status;

CREATE INDEX IF NOT EXISTS idx_legbook_status
    ON public.strategy_leg_book USING btree
    (exit_time ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_legbook_trade_entry

-- DROP INDEX IF EXISTS public.idx_legbook_trade_entry;

CREATE INDEX IF NOT EXISTS idx_legbook_trade_entry
    ON public.strategy_leg_book USING btree
    (trade_date ASC NULLS LAST, entry_time ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_legbook_trade_exit

-- DROP INDEX IF EXISTS public.idx_legbook_trade_exit;

CREATE INDEX IF NOT EXISTS idx_legbook_trade_exit
    ON public.strategy_leg_book USING btree
    (trade_date ASC NULLS LAST, exit_time ASC NULLS LAST)
    TABLESPACE pg_default;