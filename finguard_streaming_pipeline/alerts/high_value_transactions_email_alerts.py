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

def send_email_alert(customer_email, customer_name, alert_details):
    """
    Send email alert for high-value transaction.
    
    Args:
        customer_email: Recipient email address
        customer_name: Customer's name
        alert_details: Dictionary containing transaction details
    """
    try:
        # Create message
        msg = MIMEMultipart('alternative')
        msg['Subject'] = f"⚠️ High-Value Transaction Alert - {alert_details['transaction_id']}"
        msg['From'] = EMAIL_FROM
        msg['To'] = customer_email
        
        # Email body
        html_body = f"""
        <html>
          <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
            <div style="max-width: 600px; margin: 0 auto; padding: 20px; border: 1px solid #ddd; border-radius: 5px;">
              <h2 style="color: #d9534f; border-bottom: 2px solid #d9534f; padding-bottom: 10px;">
                🔔 High-Value Transaction Alert
              </h2>
              
              <p>Dear {customer_name},</p>
              
              <p>A high-value transaction has been detected on your account that exceeds your configured transaction limit.</p>
              
              <div style="background-color: #f9f9f9; padding: 15px; border-radius: 5px; margin: 20px 0;">
                <h3 style="margin-top: 0; color: #555;">Transaction Details:</h3>
                <table style="width: 100%; border-collapse: collapse;">
                  <tr>
                    <td style="padding: 8px 0; font-weight: bold; width: 40%;">Transaction ID:</td>
                    <td style="padding: 8px 0;">{alert_details['transaction_id']}</td>
                  </tr>
                  <tr>
                    <td style="padding: 8px 0; font-weight: bold;">Amount:</td>
                    <td style="padding: 8px 0; color: #d9534f; font-size: 18px; font-weight: bold;">
                      {alert_details['currency']} {alert_details['transaction_amount']:,.2f}
                    </td>
                  </tr>
                  <tr>
                    <td style="padding: 8px 0; font-weight: bold;">Your Limit:</td>
                    <td style="padding: 8px 0;">{alert_details['currency']} {alert_details['transaction_limit']:,.2f}</td>
                  </tr>
                  <tr>
                    <td style="padding: 8px 0; font-weight: bold;">Merchant:</td>
                    <td style="padding: 8px 0;">{alert_details['merchant_name']}</td>
                  </tr>
                  <tr>
                    <td style="padding: 8px 0; font-weight: bold;">Category:</td>
                    <td style="padding: 8px 0;">{alert_details['merchant_category']}</td>
                  </tr>
                  <tr>
                    <td style="padding: 8px 0; font-weight: bold;">Transaction Type:</td>
                    <td style="padding: 8px 0;">{alert_details['transaction_type']}</td>
                  </tr>
                  <tr>
                    <td style="padding: 8px 0; font-weight: bold;">Payment Channel:</td>
                    <td style="padding: 8px 0;">{alert_details['payment_channel']}</td>
                  </tr>
                  <tr>
                    <td style="padding: 8px 0; font-weight: bold;">Location:</td>
                    <td style="padding: 8px 0;">{alert_details['city']}, {alert_details['country']}</td>
                  </tr>
                  <tr>
                    <td style="padding: 8px 0; font-weight: bold;">International:</td>
                    <td style="padding: 8px 0;">{'Yes' if alert_details['is_international'] else 'No'}</td>
                  </tr>
                  <tr>
                    <td style="padding: 8px 0; font-weight: bold;">Timestamp:</td>
                    <td style="padding: 8px 0;">{alert_details['transaction_timestamp']}</td>
                  </tr>
                  <tr>
                    <td style="padding: 8px 0; font-weight: bold;">Status:</td>
                    <td style="padding: 8px 0;">{alert_details['status']}</td>
                  </tr>
                </table>
              </div>
              
              <div style="background-color: #fff3cd; padding: 15px; border-left: 4px solid #ffc107; margin: 20px 0;">
                <p style="margin: 0;"><strong>⚠️ Important:</strong></p>
                <ul style="margin: 10px 0;">
                  <li>If you recognize this transaction, no action is needed.</li>
                  <li>If you don't recognize this transaction, please contact us immediately.</li>
                  <li>You can adjust your transaction limit in your account settings.</li>
                </ul>
              </div>
              
              <p style="margin-top: 30px; color: #666; font-size: 12px;">
                This is an automated alert from FinGuard Fraud Detection System.<br>
                Alert ID: {alert_details['alert_id']}<br>
                Alert Time: {alert_details['alert_timestamp']}
              </p>
            </div>
          </body>
        </html>
        """
        
        # Attach HTML body
        msg.attach(MIMEText(html_body, 'html'))
        
        # Send email
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
          server.starttls()
          server.login(EMAIL_FROM, EMAIL_PASSWORD)
          server.send_message(msg)
            
        return True, None
        
    except Exception as e:
        return False, str(e)


@dp.foreach_batch_sink(name="high_value_transaction_email_sink")
def high_value_transaction_email_sink(df: DataFrame, batch_id: int):
    """
    ForEachBatch sink to send email alerts for high-value transactions.
    
    Args:
        df: DataFrame containing high-value transaction alerts for this batch
        batch_id: Batch identifier
    """
    # Collect alerts from this batch
    alerts = df.collect()
    
    print(f"Processing batch {batch_id} with {len(alerts)} high-value transaction alerts")
    
    success_count = 0
    failure_count = 0
    
    for alert in alerts:
        # Extract alert details
        alert_details = {
            'alert_id': alert.alert_id,
            'alert_timestamp': alert.alert_timestamp,
            'transaction_id': alert.transaction_id,
            'transaction_amount': alert.transaction_amount,
            'transaction_limit': alert.transaction_limit,
            'currency': alert.currency,
            'merchant_name': alert.merchant_name,
            'merchant_category': alert.merchant_category,
            'transaction_type': alert.transaction_type,
            'payment_channel': alert.payment_channel,
            'city': alert.city,
            'country': alert.country,
            'is_international': alert.is_international,
            'transaction_timestamp': alert.transaction_timestamp,
            'status': alert.status
        }
        
        # Send email
        success, error = send_email_alert(
            customer_email=alert.customer_email,
            customer_name=alert.customer_name,
            alert_details=alert_details
        )
       
        if not success:
            raise Exception(f"Email sending failed: {error}")
        
        if success:
            success_count += 1
            print(f"✓ Email sent successfully to {alert.customer_email} for transaction {alert.transaction_id}")
        else:
            failure_count += 1
            print(f"✗ Failed to send email to {alert.customer_email} for transaction {alert.transaction_id}: {error}")
    
    print(f"Batch {batch_id} complete: {success_count} emails sent, {failure_count} failures")


@dp.append_flow(target="high_value_transaction_email_sink")
def high_value_transaction_email_flow() -> DataFrame:
    """
    Read high-value transaction alerts and route to email sink.
    """
    return spark.readStream.table("finguard.gold.high_value_transactions")
