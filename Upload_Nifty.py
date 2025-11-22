import os
import pandas as pd
import streamlit as st
import psycopg2

st.title("CSV to PostgreSQL: Nifty50 Uploader")

folder_path = st.text_input("Enter folder path containing CSV files:")

if folder_path:
    csv_files = [f for f in os.listdir(folder_path) if f.lower().endswith('.csv')]
    st.write("CSV files found:", csv_files)

if st.button("Upload to Nifty50") and folder_path:
    conn = psycopg2.connect("dbname=Nifty_ORB_Backtest user=postgres password=Alliswell@28 host=localhost port=5432")
    cursor = conn.cursor()
    success_count = 0
    for filename in csv_files:
        try:
            file_path = os.path.join(folder_path, filename)
            df = pd.read_csv(file_path)
            # Filter rows where strike == 'Index'
            index_rows = df[df['strike'] == 'Index'].copy()
            # Convert date from DD-MM-YYYY to YYYY-MM-DD
            index_rows['date'] = pd.to_datetime(index_rows['date'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')
            for _, row in index_rows.iterrows():
                cursor.execute("""
                    INSERT INTO public."Nifty50"
                    (date, time, open, high, low, close, volume, oi, option_nm)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row['date'],
                    row['time'],
                    row['open'],
                    row['high'],
                    row['low'],
                    row['close'],
                    row['volume'],
                    row['OI'],
                    'N'  # Fixed: adjust if needed!
                ))
            success_count += 1
        except Exception as e:
            st.error(f"Failed to process {filename}: {str(e)}")
    conn.commit()
    cursor.close()
    conn.close()
    st.success(f"Processed {success_count} files successfully!")
