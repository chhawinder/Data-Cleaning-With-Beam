import streamlit as st
import pandas as pd
import os
import matplotlib.pyplot as plt
from beam_pipeline import run_beam_pipeline  # Beam for cleaning

# -------- Streamlit UI -------- #
st.title("📊 Real-Time Data Cleaning Tool")
st.write("Upload a CSV file, and we'll analyze and clean it for you!")

uploaded_file = st.file_uploader("Choose a CSV file", type=["csv"])

if uploaded_file:
    input_path = "uploaded_input.csv"
    output_prefix = "cleaned_output"

    # **Convert uploaded file to DataFrame**
    df = pd.read_csv(uploaded_file)

    # **Store original headers**
    original_headers = df.columns.tolist()

    # **Display Initial Data Summary (Before Cleaning)**
    st.write("### 🔍 Data Analysis Before Cleaning:")
    
    # **Show null values per column**
    null_counts = df.isnull().sum().to_dict()
    st.write("#### Null Values Per Column:")
    summary_df = pd.DataFrame.from_dict(null_counts, orient="index", columns=["Null Count"])
    st.dataframe(summary_df)

    # **Plot null values bar chart**
    if any(null_counts.values()):
        fig, ax = plt.subplots()
        ax.bar(null_counts.keys(), null_counts.values(), color='skyblue')
        ax.set_title("Null Values Per Column")
        ax.set_ylabel("Count")
        ax.set_xticklabels(null_counts.keys(), rotation=45)
        st.pyplot(fig)
    else:
        st.write("✅ No null values found!")

    # **Show duplicate count**
    duplicate_count = df.duplicated().sum()
    if duplicate_count > 0:
        st.warning(f"⚠️ Found **{duplicate_count}** duplicate rows!")
    else:
        st.write("✅ No duplicates found!")

    # **Save uploaded file locally with original headers**
    df.to_csv(input_path, index=False)

    # **Call backend for cleaning**
    st.success("✅ Initial analysis complete! Now processing data cleaning...")
    run_beam_pipeline(input_path, output_prefix)

    # Load cleaned data
    cleaned_file_path = output_prefix + "-00000-of-00001.csv"

    # Ensure fresh data is loaded
    if os.path.exists(cleaned_file_path):
        df_cleaned = pd.read_csv(cleaned_file_path, header=None)  # Read without auto headers

        # **Manually assign original headers**
        df_cleaned.columns = original_headers

        st.write("### ✅ Cleaned Data Preview:")
        st.dataframe(df_cleaned)
    else:
        st.error("❌ Processing failed! No cleaned output generated.")
