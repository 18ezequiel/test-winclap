# Test-winclap

How I Completed Test Number 4

## HTML Configuration for Data Extraction with Google Analytics 4

Here, I'll walk you through the HTML configuration I implemented on the website to effectively extract data using Google Analytics 4.
![Captura desde 2023-08-27 00-54-40](https://github.com/18ezequiel/test-winclap/assets/107936664/15719b14-ee27-40ce-a43a-208c371caca1)



# Demonstration of the Configured DAG in Airflow

Below, you'll find a link to a video showcasing how the configured DAG works in Airflow:

https://www.youtube.com/watch?v=Jt3DwV9nqSQ

# Week-over-Week Report:

The SQL query calculates week-over-week changes in website metrics using data from the "ga-analytics-397004.ga_analytics_397004_ezequiel.table_testing_1" table. It forms a new dataset using a common table expression (CTE) named "weekly_metrics." This CTE includes columns for the week, session medium, country, average session duration, and active users. Additionally, it utilizes the LAG() window function to calculate the previous week's average session duration and active users for each combination of session medium and country.

The final SELECT statement combines these calculated values with the original metrics. It calculates the percentage change in average session duration and active users compared to the previous week's values. The results provide insights into week-over-week performance changes in terms of user engagement and activity.

In summary, the SQL query transforms raw Google Analytics data into a meaningful representation of week-over-week changes in key metrics, facilitating the analysis of trends and performance variations.

![Captura desde 2023-08-27 02-02-50](https://github.com/18ezequiel/test-winclap/assets/107936664/2fb964f5-5d71-4e60-93c3-c337e94a7490)


# Links

To conclude, I will provide the link to access the project permissions. These permissions should be placed within the directory "test_4/airflow-docker". 

https://drive.google.com/file/d/1Il1x1D_41s1GZwu1FdHd17s_cZxCfmTn/view?usp=drive_link

Thank you very much for your kind attention.
