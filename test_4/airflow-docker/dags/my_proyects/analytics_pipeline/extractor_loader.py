from google.cloud import bigquery
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest, OrderBy
import pandas as pd
import numpy as np
import time
import os

# Class to handle data extraction and loading
class AnalyticsExtractorAndLoader:
    def __init__(self, credentials_file, property_id):
        self.credentials_file = credentials_file
        self.property_id = property_id
        self.client = BetaAnalyticsDataClient()

    # Method to extract data using Google Analytics API
    def extract_data(self, dimensions, metrics, order_bys, date_range):
        # Create a RunReportRequest to fetch data
        request = RunReportRequest(
            property='properties/' + self.property_id,
            dimensions=dimensions,
            metrics=metrics,
            order_bys=order_bys,
            date_ranges=[date_range],
        )

        # Execute the request and receive the response
        response = self.client.run_report(request)

        # Process the response to create a DataFrame
        row_index_names = [header.name for header in response.dimension_headers]
        row_header = []
        for i in range(len(row_index_names)):
            row_header.append([row.dimension_values[i].value for row in response.rows])

        row_index_named = pd.MultiIndex.from_arrays(np.array(row_header), names=np.array(row_index_names))

        metric_names = [header.name for header in response.metric_headers]
        data_values = []
        for i in range(len(metric_names)):
            data_values.append([row.metric_values[i].value for row in response.rows])

        output = pd.DataFrame(data=np.transpose(np.array(data_values, dtype='f')),
                              index=row_index_named, columns=metric_names)

        return output

    # Method to load DataFrame into BigQuery
    def load_to_bigquery(self, df, table_id):
        client = bigquery.Client()

        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition='WRITE_TRUNCATE'
        )

        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        while job.state != 'DONE':
            time.sleep(2)
            job.reload()

        table = client.get_table(table_id)
        return table.num_rows, len(table.schema)

# Example usage
extractor_loader = AnalyticsExtractorAndLoader('ga-analytics-397004-c6f5acdcbd7a.json', '404209256')
dimensions = [
    Dimension(name="week"), 
    Dimension(name="sessionMedium"),
    Dimension(name="country")
]
metrics = [
    Metric(name="averageSessionDuration"), 
    Metric(name="activeUsers")
]
order_bys = [
    OrderBy(dimension={'dimension_name': 'week'}),
    OrderBy(dimension={'dimension_name': 'sessionMedium'})
]
date_range = DateRange(start_date="2023-08-18", end_date="today")
data_df = extractor_loader.extract_data(dimensions, metrics, order_bys, date_range)
table_id = 'ga-analytics-397004.ga_analytics_397004_ezequiel.table_testing_1'
rows_loaded, columns_loaded = extractor_loader.load_to_bigquery(data_df, table_id)
print(f'Loaded {rows_loaded} rows and {columns_loaded} columns to "{table_id}"')