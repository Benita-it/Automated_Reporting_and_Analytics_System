import pyodbc
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import smtplib
import logging
import schedule
import time
import os
import sys
import datetime
from email.message import EmailMessage
from typing import Optional


LOG_DIR = "logs"
REPORT_DIR = "reports"
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(REPORT_DIR, exist_ok=True)

log_filename = os.path.join(LOG_DIR, f"report_{datetime.datetime.now().strftime('%Y%m%d')}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


DB_CONFIG = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=your_server_ip;"
    "DATABASE=your_database_name;"
    "UID=your_username;"
    "PWD=your_password;"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
    "Connection Timeout=30;"
)

EMAIL_CONFIG = {
    "sender": "your_email@gmail.com",
    "password": "your_gmail_app_password",
    "to": ["recipient@example.com"],
    "cc": ["cc1@example.com", "cc2@example.com"],
    "subject": "Automated SQL Quotation Report",
}

REPORT_INTERVAL_MINUTES = 20
MAX_TOP_CUSTOMERS = 10
CHART_DPI = 150
CHART_STYLE = "ggplot"


def get_timestamp() -> str:
    return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")


def get_readable_time() -> str:
    return datetime.datetime.now().strftime("%d %B %Y, %I:%M %p")


def connect_to_database() -> pyodbc.Connection:
    logger.info("Establishing connection to SQL Server...")
    try:
        conn = pyodbc.connect(DB_CONFIG)
        logger.info("Database connection established successfully.")
        return conn
    except pyodbc.Error as e:
        logger.error(f"Database connection failed: {e}")
        raise


def fetch_quotation_data(conn: pyodbc.Connection) -> pd.DataFrame:
    logger.info("Executing stored procedure: sp_ReportQuotation...")
    try:
        query = "EXEC sp_ReportQuotation @FilterString='', @userId=1"
        df = pd.read_sql(query, conn)
        if df.empty:
            logger.warning("Query returned no data. Report will be empty.")
        else:
            logger.info(f"Data fetched successfully — {len(df)} rows, {len(df.columns)} columns.")
        return df
    except Exception as e:
        logger.error(f"Failed to fetch data: {e}")
        raise


def validate_dataframe(df: pd.DataFrame) -> bool:
    if df is None or df.empty:
        logger.warning("DataFrame is empty. Skipping report generation.")
        return False
    logger.info("DataFrame validation passed.")
    return True


def generate_status_chart(df: pd.DataFrame, timestamp: str) -> Optional[str]:
    if 'QStus' not in df.columns:
        logger.warning("Column 'QStus' not found. Skipping status chart.")
        return None

    try:
        plt.style.use(CHART_STYLE)
        fig, ax = plt.subplots(figsize=(10, 5))

        status_counts = df['QStus'].value_counts()
        bars = ax.bar(status_counts.index, status_counts.values, color='steelblue', edgecolor='white', linewidth=0.8)

        for bar in bars:
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 0.3,
                str(int(bar.get_height())),
                ha='center', va='bottom', fontsize=10, fontweight='bold'
            )

        ax.set_title('Quotation Status Distribution', fontsize=14, fontweight='bold', pad=15)
        ax.set_xlabel('Status', fontsize=11)
        ax.set_ylabel('Number of Quotations', fontsize=11)
        ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
        ax.tick_params(axis='x', rotation=30)
        plt.tight_layout()

        chart_path = os.path.join(REPORT_DIR, f"quotation_status_chart_{timestamp}.png")
        plt.savefig(chart_path, dpi=CHART_DPI, bbox_inches='tight')
        plt.close()
        logger.info(f"Status chart saved: {chart_path}")
        return chart_path

    except Exception as e:
        logger.error(f"Failed to generate status chart: {e}")
        return None


def generate_top_customers_chart(df: pd.DataFrame, timestamp: str) -> Optional[str]:
    if 'Client' not in df.columns:
        logger.warning("Column 'Client' not found. Skipping customers chart.")
        return None

    try:
        plt.style.use(CHART_STYLE)
        fig, ax = plt.subplots(figsize=(12, 6))

        top_customers = df['Client'].value_counts().head(MAX_TOP_CUSTOMERS)
        bars = ax.barh(top_customers.index[::-1], top_customers.values[::-1], color='darkorange', edgecolor='white', linewidth=0.8)

        for bar in bars:
            ax.text(
                bar.get_width() + 0.2,
                bar.get_y() + bar.get_height() / 2,
                str(int(bar.get_width())),
                va='center', fontsize=10, fontweight='bold'
            )

        ax.set_title(f'Top {MAX_TOP_CUSTOMERS} Customers by Quotation Volume', fontsize=14, fontweight='bold', pad=15)
        ax.set_xlabel('Number of Quotations', fontsize=11)
        ax.set_ylabel('Customer', fontsize=11)
        ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))
        plt.tight_layout()

        chart_path = os.path.join(REPORT_DIR, f"top_customers_chart_{timestamp}.png")
        plt.savefig(chart_path, dpi=CHART_DPI, bbox_inches='tight')
        plt.close()
        logger.info(f"Top customers chart saved: {chart_path}")
        return chart_path

    except Exception as e:
        logger.error(f"Failed to generate customers chart: {e}")
        return None

    try:
        plt.style.use(CHART_STYLE)
        fig, ax = plt.subplots(figsize=(10, 5))

        pl_by_status = df.groupby('QStus')['PL'].sum().sort_values()
        colors = ['#e74c3c' if v < 0 else '#2ecc71' for v in pl_by_status.values]
        bars = ax.bar(pl_by_status.index, pl_by_status.values, color=colors, edgecolor='white', linewidth=0.8)

        for bar in bars:
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + (max(pl_by_status.values) * 0.01),
                f"{bar.get_height():,.0f}",
                ha='center', va='bottom', fontsize=9, fontweight='bold'
            )

        chart_path = os.path.join(REPORT_DIR, f"profit_loss_chart_{timestamp}.png")
        plt.savefig(chart_path, dpi=CHART_DPI, bbox_inches='tight')
        plt.close()
        logger.info(f"Profit/loss chart saved: {chart_path}")
        return chart_path

    except Exception as e:
        logger.error(f"Failed to generate profit/loss chart: {e}")
        return None


def generate_all_charts(df: pd.DataFrame, timestamp: str) -> list:
    logger.info("Generating charts...")
    charts = []
    for generator in [generate_status_chart, generate_top_customers_chart, generate_profit_loss_chart]:
        result = generator(df, timestamp)
        if result:
            charts.append(result)
    logger.info(f"{len(charts)} chart(s) generated successfully.")
    return charts


def export_to_excel(df: pd.DataFrame, timestamp: str) -> str:
    excel_path = os.path.join(REPORT_DIR, f"quotation_report_{timestamp}.xlsx")
    try:
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Quotation Report')

            workbook = writer.book
            worksheet = writer.sheets['Quotation Report']

            for col in worksheet.columns:
                max_length = max((len(str(cell.value)) for cell in col if cell.value), default=10)
                worksheet.column_dimensions[col[0].column_letter].width = min(max_length + 4, 40)

        logger.info(f"Excel report exported: {excel_path} ({len(df)} rows)")
        return excel_path
    except Exception as e:
        logger.error(f"Failed to export Excel report: {e}")
        raise


def build_summary_stats(df: pd.DataFrame) -> str:
    lines = []
    total = len(df)
    lines.append(f"  Total Quotations   : {total}")

    if 'QStus' in df.columns:
        for status, count in df['QStus'].value_counts().items():
            pct = (count / total) * 100
            lines.append(f"  {status:<20}: {count} ({pct:.1f}%)")

    if 'Client' in df.columns:
        top_client = df['Client'].value_counts().idxmax()
        lines.append(f"  Top Client         : {top_client}")

    return "\n".join(lines)


def build_email(excel_file: str, chart_files: list, summary: str) -> EmailMessage:
    msg = EmailMessage()
    msg["Subject"] = f"{EMAIL_CONFIG['subject']} — {get_readable_time()}"
    msg["From"] = EMAIL_CONFIG["sender"]
    msg["To"] = ", ".join(EMAIL_CONFIG["to"])
    msg["Cc"] = ", ".join(EMAIL_CONFIG["cc"])

    body = (
        f"Hi Team,\n\n"
        f"Please find attached the latest automated Quotation Report generated on {get_readable_time()},Regards Benita.\n\n"
        f"── REPORT SUMMARY ──────────────────────────\n"
        f"{summary}\n"
        f"────────────────────────────────────────────\n\n"
        f"Attachments include:\n"
        f"  • Excel Report     — Full quotation data\n"
        f"  • Status Chart     — Quotation distribution by status\n"
        f"  • Customers Chart  — Top {MAX_TOP_CUSTOMERS} customers by volume\n"
        f"This report is auto-generated every {REPORT_INTERVAL_MINUTES} minutes.\n"
        f"Regards,\nBenita\nAutomated Reporting System"
    )

    msg.set_content(body)

    with open(excel_file, "rb") as f:
        msg.add_attachment(
            f.read(),
            maintype="application",
            subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=os.path.basename(excel_file)
        )

    for chart in chart_files:
        with open(chart, "rb") as f:
            msg.add_attachment(
                f.read(),
                maintype="image",
                subtype="png",
                filename=os.path.basename(chart)
            )

    return msg


def send_email(msg: EmailMessage) -> None:
    all_recipients = EMAIL_CONFIG["to"] + EMAIL_CONFIG["cc"]
    try:
        logger.info(f"Sending email to {len(all_recipients)} recipient(s)...")
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(EMAIL_CONFIG["sender"], EMAIL_CONFIG["password"])
            server.send_message(msg, to_addrs=all_recipients)
        logger.info("Email sent successfully.")
    except smtplib.SMTPException as e:
        logger.error(f"Failed to send email: {e}")
        raise


def cleanup_old_reports(days: int = 7) -> None:
    cutoff = datetime.datetime.now() - datetime.timedelta(days=days)
    removed = 0
    for folder in [REPORT_DIR, LOG_DIR]:
        for filename in os.listdir(folder):
            filepath = os.path.join(folder, filename)
            if os.path.isfile(filepath):
                file_time = datetime.datetime.fromtimestamp(os.path.getmtime(filepath))
                if file_time < cutoff:
                    os.remove(filepath)
                    removed += 1
    if removed:
        logger.info(f"Cleanup complete — {removed} old file(s) removed (older than {days} days).")


def generate_and_send_report() -> None:
    logger.info("=" * 55)
    logger.info("REPORT JOB STARTED")
    logger.info("=" * 55)

    start_time = datetime.datetime.now()

    try:
        conn = connect_to_database()
        df = fetch_quotation_data(conn)
        conn.close()
        logger.info("Database connection closed.")

        if not validate_dataframe(df):
            return

        timestamp = get_timestamp()
        summary = build_summary_stats(df)
        chart_files = generate_all_charts(df, timestamp)
        excel_file = export_to_excel(df, timestamp)
        msg = build_email(excel_file, chart_files, summary)
        send_email(msg)
        cleanup_old_reports(days=7)

        elapsed = (datetime.datetime.now() - start_time).seconds
        logger.info("=" * 55)
        logger.info(f"REPORT JOB COMPLETED SUCCESSFULLY in {elapsed}s")
        logger.info("=" * 55)

    except Exception as e:
        logger.error("=" * 55)
        logger.error(f"REPORT JOB FAILED: {e}")
        logger.error("=" * 55)


if __name__ == "__main__":
    logger.info("Automated Quotation Report System — Starting up...")
    logger.info(f"Reports will run every {REPORT_INTERVAL_MINUTES} minutes.")
    logger.info(f"Logs saved to: {log_filename}")

    generate_and_send_report()

    schedule.every(REPORT_INTERVAL_MINUTES).minutes.do(generate_and_send_report)

    while True:
        schedule.run_pending()
        time.sleep(60)
