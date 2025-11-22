import os
import pandas as pd
import streamlit as st
import psycopg2

st.title("CSV to PostgreSQL: Nifty_options Uploader")

folder_path = st.text_input("Enter folder path containing CSV files:")

if folder_path:
    csv_files = [f for f in os.listdir(folder_path) if f.lower().endswith('.csv')]
    st.write("CSV files found:", csv_files)

if st.button("Upload to Nifty_options") and folder_path:
    try:
        conn = psycopg2.connect("dbname=Nifty_ORB_Backtest user=postgres password=Alliswell@28 host=localhost port=5432")
        cursor = conn.cursor()
        success_count = 0
        for filename in csv_files:
            file_path = os.path.join(folder_path, filename)
            df = pd.read_csv(file_path)            
            # Filter out rows where strike is 'Index'
            df_filtered = df[df['strike'].astype(str).str.upper() != 'INDEX']
            # Convert date and expiry columns from DD-MM-YYYY to YYYY-MM-DD            
            if 'date' in df.columns:
                df_filtered['date'] = pd.to_datetime(df_filtered['date'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')
            if 'expiry' in df.columns:
                df_filtered['expiry'] = pd.to_datetime(df_filtered['expiry'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')
            for _, row in df_filtered.iterrows():
                # Option Type Mapping: PE -> P, CE -> C
                option_type_db = (
                    'P' if str(row['option_type']).strip().upper() == 'PE'
                    else 'C' if str(row['option_type']).strip().upper() == 'CE'
                    else row['option_type']
                )
                cursor.execute("""
INSERT INTO public."Nifty_options"
(symbol, date, expiry, strike, option_type, time, open, high, low, close, volume, oi, option_nm)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)

                """, (
                    'N',
                    row['date'],
                    row['expiry'],
                    row['strike'],
                    option_type_db,
                    row['time'],
                    row['open'],
                    row['high'],
                    row['low'],
                    row['close'],
                    row['volume'],
                    row['OI'],
                    'N'
                ))
            success_count += 1
        conn.commit()
        cursor.close()
        conn.close()
        st.success(f"Processed {success_count} files successfully!")
    except Exception as e:
        st.error(f"Error: {str(e)}")
