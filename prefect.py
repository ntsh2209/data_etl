from prefect import flow, task
import pandas as pd
from utils.reconciliation import reconcile_data
from utils.report_generator import generate_report
from utils.emailer import send_email_report

@task
def load_data():
    input_df = pd.read_csv("input_data.csv")
    golden_df = pd.read_csv("golden_data.csv")
    return input_df, golden_df

@task
def run_reconciliation(input_df, golden_df):
    report_df, breached = reconcile_data(input_df, golden_df)
    return report_df, breached

@task
def generate_and_email(report_df, breached):
    if breached:
        generate_report(report_df)
        send_email_report(report_df)

@flow(name="Stock Data Reconciliation Pipeline")
def stock_data_pipeline():
    input_df, golden_df = load_data()
    report_df, breached = run_reconciliation(input_df, golden_df)
    generate_and_email(report_df, breached)

if __name__ == "__main__":
    stock_data_pipeline()
