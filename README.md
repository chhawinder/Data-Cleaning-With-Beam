# Real-Time Data Cleaning Tool

## 📌 Why Use This Project?
Cleaning and preprocessing data is an essential step in data analysis and machine learning. This tool automates the cleaning process by leveraging **Apache Beam** for backend data processing and **Streamlit** for an interactive UI. With this, you can:
- Handle missing values in numeric and categorical columns.
- Remove duplicates.
- Analyze null values before and after cleaning.
- View real-time visualizations of data issues.

---

## 🚀 Installation Guide

### Step 1: Clone This Repository
```sh
git clone https://github.com/chhawinder/Data-Cleaning-With-Beam.git
cd Data-Cleaning-With-Beam
```

### Step 2: Install Dependencies
Make sure you have Python installed (recommended: Python 3.8+). Then, install required packages:
```sh
pip install -r requirements.txt
```

### Step 3: Run the Streamlit App
Start the application with:
```sh
streamlit run app.py
```

### Step 4: Test the Model
1. Upload a CSV file via the Streamlit UI.
2. View initial data analysis (missing values, duplicates, etc.).
3. Click to process the file through Apache Beam.
4. See the cleaned data output instantly!

---

## 📂 File Structure
```
├── beam_pipeline.py       # Apache Beam data cleaning logic
├── app.py                 # Streamlit frontend for data upload and visualization
├── requirements.txt       # Required dependencies
├── README.md              # Documentation
```

---

## 🔧 How It Works
- **Backend Processing:** Uses Apache Beam to handle missing values and remove duplicates.
- **Frontend UI:** Built with Streamlit to provide an interactive data upload, analysis, and visualization experience.
- **Automated Cleaning:** Numeric columns get missing values replaced with their mean, while string columns replace missing values with "empty".

---

## 🛠 Technologies Used
- **Apache Beam** (for scalable data processing)
- **Streamlit** (for interactive UI)
- **Pandas & Matplotlib** (for data manipulation and visualization)
- **Logging** (for error handling and debugging)

---

## 📞 Contact
For issues or contributions, create a GitHub issue or reach out to the repository owner.

Happy Cleaning! 🚀

