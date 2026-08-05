from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

EMAIL_FROM = "k.pradeepkumarreddy@gmail.com"
EMAIL_PASSWORD = dbutils.secrets.get("finguard-secrets-scope", "gmail-app-password")
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

def send_fraud_email(customer_email, customer_name, fraud_details_list):
    """
    Send email alert for fraud transactions.

    Args:
        customer_email: Recipient email address
        customer_name: Customer's name
        fraud_details_list: List of dictionaries containing fraud transaction details
    """
    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = f"🚨 Fraud Transaction Alert - {len(fraud_details_list)} Detected"
        msg['From'] = EMAIL_FROM
        msg['To'] = customer_email

        # Build HTML table for fraud transactions
        rows_html = ""
        for fraud in fraud_details_list:
            rows_html += f"""
            <tr>
                <td style="padding:8px;border:1px solid #ddd;">{fraud['transaction_id']}</td>
                <td style="padding:8px;border:1px solid #ddd;">{fraud['amount']} {fraud['currency']}</td>
                <td style="padding:8px;border:1px solid #ddd;">{fraud['merchant_name']}</td>
                <td style="padding:8px;border:1px solid #ddd;">{fraud['transaction_type']}</td>
                <td style="padding:8px;border:1px solid #ddd;">{fraud['transaction_city']}, {fraud['transaction_country']}</td>
                <td style="padding:8px;border:1px solid #ddd;">{fraud['transaction_timestamp']}</td>
                <td style="padding:8px;border:1px solid #ddd;">{fraud['alert_type']}</td>
                <td style="padding:8px;border:1px solid #ddd;">{fraud['risk_level']}</td>
                <td style="padding:8px;border:1px solid #ddd;">{fraud['action']}</td>
                <td style="padding:8px;border:1px solid #ddd;">{fraud['reason_description']}</td>
            </tr>
            """

        html_body = f"""
        <html>
          <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
            <div style="max-width: 700px; margin: 0 auto; padding: 20px; border: 1px solid #ddd; border-radius: 5px;">
              <h2 style="color: #d9534f; border-bottom: 2px solid #d9534f; padding-bottom: 10px;">
                🚨 Fraud Transaction Alert
              </h2>
              <p>Dear {customer_name},</p>
              <p>The following fraud transactions have been detected on your account:</p>
              <table style="width:100%;border-collapse:collapse;margin-top:20px;">
                <thead>
                  <tr style="background-color:#f2dede;">
                    <th style="padding:8px;border:1px solid #ddd;">Transaction ID</th>
                    <th style="padding:8px;border:1px solid #ddd;">Amount</th>
                    <th style="padding:8px;border:1px solid #ddd;">Merchant</th>
                    <th style="padding:8px;border:1px solid #ddd;">Type</th>
                    <th style="padding:8px;border:1px solid #ddd;">Location</th>
                    <th style="padding:8px;border:1px solid #ddd;">Timestamp</th>
                    <th style="padding:8px;border:1px solid #ddd;">Alert Type</th>
                    <th style="padding:8px;border:1px solid #ddd;">Risk Level</th>
                    <th style="padding:8px;border:1px solid #ddd;">Action</th>
                    <th style="padding:8px;border:1px solid #ddd;">Reason</th>
                  </tr>
                </thead>
                <tbody>
                  {rows_html}
                </tbody>
              </table>
              <div style="background-color: #fff3cd; padding: 15px; border-left: 4px solid #ffc107; margin: 20px 0;">
                <p style="margin: 0;"><strong>⚠️ Important:</strong></p>
                <ul style="margin: 10px 0;">
                  <li>If you recognize these transactions, no action is needed.</li>
                  <li>If you don't recognize any of these transactions, please contact us immediately.</li>
                </ul>
              </div>
              <p style="margin-top: 30px; color: #666; font-size: 12px;">
                This is an automated alert from FinGuard Fraud Detection System.
              </p>
            </div>
          </body>
        </html>
        """

        msg.attach(MIMEText(html_body, 'html'))

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_FROM, EMAIL_PASSWORD)
            server.send_message(msg)

        return True, None

    except Exception as e:
        return False, str(e)


@dp.foreach_batch_sink(name="fraud_transaction_email_sink")
def fraud_transaction_email_sink(df: DataFrame, batch_id: int):
    """
    ForEachBatch sink to send email alerts for fraud transactions.

    Args:
        df: DataFrame containing fraud transaction alerts for this batch
        batch_id: Batch identifier
    """
    alerts = df.collect()
    print(f"Processing batch {batch_id} with {len(alerts)} fraud transaction alerts")

    # Group fraud transactions by customer_email and customer_name
    from collections import defaultdict
    customer_fraud_map = defaultdict(list)
    for alert in alerts:
        fraud_details = {
            'alert_id': alert.alert_id,
            'alert_type': alert.alert_type,
            'alert_timestamp': alert.alert_timestamp,
            'transaction_id': alert.transaction_id,
            'amount': alert.amount,
            'currency': alert.currency,
            'merchant_name': alert.merchant_name,
            'merchant_category': alert.merchant_category,
            'transaction_type': alert.transaction_type,
            'payment_channel': alert.payment_channel,
            'transaction_city': alert.transaction_city,
            'transaction_country': alert.transaction_country,
            'transaction_timestamp': alert.transaction_timestamp,
            'is_international': alert.is_international,
            'transaction_status': alert.transaction_status,
            'risk_level': alert.risk_level,
            'action': alert.action,
            'reason_code': alert.reason_code,
            'reason_description': alert.reason_description
        }
        customer_fraud_map[(alert.customer_email, alert.customer_name)].append(fraud_details)

    success_count = 0
    failure_count = 0

    for (customer_email, customer_name), fraud_details_list in customer_fraud_map.items():
        success, error = send_fraud_email(
            customer_email=customer_email,
            customer_name=customer_name,
            fraud_details_list=fraud_details_list
        )
        if not success:
            raise Exception(f"Email sending failed: {error}")
        if success:
            success_count += 1
            print(f"✓ Email sent successfully to {customer_email} with {len(fraud_details_list)} fraud transactions")
        else:
            failure_count += 1
            print(f"✗ Failed to send email to {customer_email}: {error}")

    print(f"Batch {batch_id} complete: {success_count} emails sent, {failure_count} failures")


@dp.append_flow(target="fraud_transaction_email_sink")
def fraud_transaction_email_flow() -> DataFrame:
    """
    Read fraud transaction alerts and route to email sink.
    """
    return spark.readStream.table("finguard.gold.fraud_transactions")
