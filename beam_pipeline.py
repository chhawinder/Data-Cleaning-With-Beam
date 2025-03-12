import apache_beam as beam
import csv
import os
import logging
from apache_beam.options.pipeline_options import PipelineOptions
import numpy as np

# Setup logging for debugging
logging.basicConfig(level=logging.DEBUG)

class ComputeColumnStats(beam.DoFn):
    """Compute averages for numeric columns"""
    
    def process(self, element):
        try:
            row_dict = dict(zip(self.headers, element))
            for key, value in row_dict.items():
                if value.replace(".", "", 1).isdigit():  # Check if numeric (including floats)
                    row_dict[key] = float(value)
                else:
                    row_dict[key] = None  # Mark non-numeric as None for now
            yield row_dict
        except Exception as e:
            logging.error(f"Error processing row {element}: {str(e)}", exc_info=True)

class CleanDataFn(beam.DoFn):
    """Data Cleaning Transform with Header Handling"""

    def __init__(self, headers, column_averages):
        self.headers = headers
        self.column_averages = column_averages  # Dictionary of column-wise averages

    def process(self, element):
        try:
            row_dict = dict(zip(self.headers, element))

            # Normalize: Convert to lowercase, strip spaces
            for key in row_dict:
                if isinstance(row_dict[key], str):
                    row_dict[key] = row_dict[key].strip().lower()

            # Handle missing values
            for key, value in row_dict.items():
                if value == "" or value is None:
                    if key in self.column_averages:  # Numeric column
                        row_dict[key] = self.column_averages[key]
                    else:  # String column
                        row_dict[key] = "empty"

            yield row_dict

        except Exception as e:
            logging.error(f"Error processing row {element}: {str(e)}", exc_info=True)

def format_csv_line(row_dict):
    """Convert dictionary to CSV formatted line"""
    return ",".join(str(row_dict[key]) for key in row_dict)

def run_beam_pipeline(input_path, output_prefix):
    """Runs Apache Beam pipeline on given CSV file, ensuring old files are removed"""
    try:
        options = PipelineOptions()

        # Ensure old files are removed before running the pipeline
        for suffix in ["-00000-of-00001.csv"]:
            old_file = f"{output_prefix}{suffix}"
            if os.path.exists(old_file):
                os.remove(old_file)
                logging.info(f"Removed old file: {old_file}")

        # Read headers separately to pass them to DoFn
        with open(input_path, "r") as f:
            reader = csv.reader(f)
            headers = next(reader)

        with beam.Pipeline(options=options) as p:
            logging.info(f"Starting Beam Pipeline with input: {input_path}")

            raw_data = (
                p | "ReadCSV" >> beam.io.ReadFromText(input_path, skip_header_lines=1)
                  | "Split" >> beam.Map(lambda x: x.split(","))  # Convert CSV line to list
            )

            # Convert to numeric and collect column statistics
            column_stats = (
                raw_data
                | "ComputeColumnStats" >> beam.ParDo(ComputeColumnStats(headers))
                | "GroupByKey" >> beam.CombineGlobally(lambda rows: {
                    key: np.mean([row[key] for row in rows if isinstance(row[key], (int, float))]) 
                    for key in headers
                })  # Compute column-wise averages
            )

            cleaned_data = (
                raw_data
                | "CleanData" >> beam.ParDo(CleanDataFn(headers, beam.pvalue.AsSingleton(column_stats)))
            )

            formatted_output = (
                cleaned_data | "FormatToCSV" >> beam.Map(format_csv_line)
                             | "WriteCSV" >> beam.io.WriteToText(output_prefix, file_name_suffix=".csv")
            )

        logging.info(f"Pipeline executed successfully! Output saved at: {output_prefix}")

    except Exception as e:
        logging.error(f"Pipeline execution failed: {str(e)}", exc_info=True)
