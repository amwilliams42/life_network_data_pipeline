"""
Email Tasks - Shared email functionality for Prefect flows

Provides reusable tasks for sending emails with attachments via SMTP.
Uses Prefect blocks for credentials and variables for recipient lists.

Test usage:
    python -m flows.email test@example.com
    python -m flows.email test@example.com "Custom subject" "Custom body"
"""

from pathlib import Path

from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.variables import Variable
from prefect_email import EmailServerCredentials, email_send_message


@task
def get_email_recipients(variable_name: str) -> list[str]:
    """
    Get email recipients from a Prefect variable.

    Args:
        variable_name: Name of the Prefect variable containing recipient list.
                      Variable should be a JSON list of email addresses.

    Returns:
        List of email addresses, or empty list if variable not found.
    """
    logger = get_run_logger()
    recipients = Variable.get(variable_name, default=[])

    # Handle single string or list
    if isinstance(recipients, str):
        recipients = [recipients]

    if recipients:
        logger.info(f"Loaded {len(recipients)} recipients from '{variable_name}'")
    else:
        logger.warning(f"No recipients found in variable '{variable_name}'")

    return recipients


@task
def send_email(
    recipients: list[str],
    subject: str,
    body: str,
    attachments: list[Path | str] | None = None,
    email_block_name: str = "smtp-credentials",
    email_from: str | None = None,
) -> bool:
    """
    Send an email with optional attachments.

    Args:
        recipients: List of email addresses to send to.
        subject: Email subject line.
        body: Email body text.
        attachments: Optional list of file paths to attach.
        email_block_name: Name of the EmailServerCredentials block.
        email_from: Optional sender email address. If not provided,
                   uses the username from credentials.

    Returns:
        True if email sent successfully, False otherwise.
    """
    import smtplib
    import ssl
    from email.message import EmailMessage
    from email.mime.base import MIMEBase
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email import encoders

    logger = get_run_logger()
    
    logger.info(f"send_email called:")
    logger.info(f"  recipients: {recipients}")
    logger.info(f"  subject: {subject}")
    logger.info(f"  attachments: {attachments}")
    logger.info(f"  email_block_name: {email_block_name}")

    if not recipients:
        logger.warning("No recipients specified, skipping email")
        return False

    try:
        logger.info(f"Loading credentials from block: {email_block_name}")
        credentials = EmailServerCredentials.load(email_block_name)
        
        sender = email_from or credentials.username
        logger.info(f"Sender: {sender}")
        logger.info(f"SMTP Server: {credentials.smtp_server}")
        logger.info(f"SMTP Port: {credentials.smtp_port}")
        logger.info(f"SMTP Type: {credentials.smtp_type}")

        # Build the email message
        logger.info("Building email message...")
        if attachments:
            logger.info(f"Building multipart message with {len(attachments)} attachment(s)")
            msg = MIMEMultipart()
            msg.attach(MIMEText(body, "plain"))
            
            for attachment_path in attachments:
                path = Path(attachment_path)
                logger.info(f"Attaching file: {path} (exists: {path.exists()}, size: {path.stat().st_size if path.exists() else 'N/A'} bytes)")
                with open(path, "rb") as f:
                    part = MIMEBase("application", "octet-stream")
                    part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header(
                    "Content-Disposition",
                    f"attachment; filename={path.name}",
                )
                msg.attach(part)
        else:
            logger.info("Building simple message (no attachments)")
            msg = EmailMessage()
            msg.set_content(body)

        msg["Subject"] = subject
        msg["From"] = sender
        msg["To"] = ", ".join(recipients)
        logger.info(f"Message headers set: From={sender}, To={msg['To']}, Subject={subject}")

        # Create SSL context and send
        context = ssl.create_default_context()
        
        smtp_type = str(credentials.smtp_type).upper()
        logger.info(f"Connecting to {credentials.smtp_server}:{credentials.smtp_port} ({smtp_type})")
        
        if "SSL" in smtp_type:
            logger.info("Using SMTP_SSL connection...")
            with smtplib.SMTP_SSL(
                credentials.smtp_server, 
                credentials.smtp_port, 
                context=context
            ) as server:
                logger.info("Connected, logging in...")
                server.login(credentials.username, credentials.password.get_secret_value())
                logger.info("Logged in, sending message...")
                server.send_message(msg)
                logger.info(f"Message sent successfully to {len(recipients)} recipients: {subject}")
        else:
            # STARTTLS
            logger.info("Using SMTP with STARTTLS...")
            with smtplib.SMTP(credentials.smtp_server, credentials.smtp_port) as server:
                logger.info("Connected, starting TLS...")
                server.starttls(context=context)
                logger.info("TLS started, logging in...")
                server.login(credentials.username, credentials.password.get_secret_value())
                logger.info("Logged in, sending message...")
                server.send_message(msg)
                logger.info(f"Message sent successfully to {len(recipients)} recipients: {subject}")
        
        return True

    except smtplib.SMTPException as e:
        logger.error(f"SMTP error sending email: {e}")
        return False
    except Exception as e:
        logger.error(f"Failed to send email: {type(e).__name__}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


@task
def send_report_email(
    filepath: Path,
    recipients: list[str],
    subject: str,
    body: str,
    email_block_name: str = "smtp-credentials",
) -> bool:
    """
    Send an email with a single report attachment.

    Convenience wrapper around send_email for single-file reports.

    Args:
        filepath: Path to the report file to attach.
        recipients: List of email addresses.
        subject: Email subject line.
        body: Email body text.
        email_block_name: Name of the EmailServerCredentials block.

    Returns:
        True if email sent successfully, False otherwise.
    """
    logger = get_run_logger()
    logger.info(f"send_report_email called:")
    logger.info(f"  filepath: {filepath}")
    logger.info(f"  filepath exists: {Path(filepath).exists()}")
    logger.info(f"  recipients: {recipients}")
    logger.info(f"  subject: {subject}")
    logger.info(f"  email_block_name: {email_block_name}")
    
    result = send_email(
        recipients=recipients,
        subject=subject,
        body=body,
        attachments=[filepath],
        email_block_name=email_block_name,
    )
    logger.info(f"send_email returned: {result}")
    return result


@task
def send_email_from_variable(
    variable_name: str,
    subject: str,
    body: str,
    attachments: list[Path | str] | None = None,
    email_block_name: str = "smtp-credentials",
) -> bool:
    """
    Send an email to recipients defined in a Prefect variable.

    Combines get_email_recipients and send_email into a single task.

    Args:
        variable_name: Name of Prefect variable containing recipient list.
        subject: Email subject line.
        body: Email body text.
        attachments: Optional list of file paths to attach.
        email_block_name: Name of the EmailServerCredentials block.

    Returns:
        True if email sent successfully, False otherwise.
    """
    logger = get_run_logger()

    recipients = Variable.get(variable_name, default=[])
    if isinstance(recipients, str):
        recipients = [recipients]

    if not recipients:
        logger.warning(f"No recipients in variable '{variable_name}', skipping email")
        return False

    return send_email(
        recipients=recipients,
        subject=subject,
        body=body,
        attachments=attachments,
        email_block_name=email_block_name,
    )


# =============================================================================
# Test Flow
# =============================================================================

@flow
def send_test_email(
    to: str,
    subject: str = "Test Email from Prefect",
    body: str | None = None,
    email_block_name: str = "smtp-credentials",
) -> bool:
    """
    Send a test email to verify SMTP configuration.

    Args:
        to: Email address to send test email to.
        subject: Email subject (default: "Test Email from Prefect")
        body: Email body (default: auto-generated with timestamp)
        email_block_name: Name of the EmailServerCredentials block.

    Example:
        # From CLI
        python -m flows.email test@example.com

        # From Python
        from flows.email import send_test_email
        send_test_email(to="test@example.com")

        # Via Prefect
        prefect deployment run 'send-test-email/default' --param to=test@example.com
    """
    import datetime

    from prefect_email import EmailServerCredentials

    logger = get_run_logger()
    logger.info(f"Sending test email to {to}")

    # Log SMTP configuration for debugging
    try:
        creds = EmailServerCredentials.load(email_block_name)
        logger.info(f"SMTP Server: {creds.smtp_server}")
        logger.info(f"SMTP Port: {creds.smtp_port}")
        logger.info(f"SMTP Type: {creds.smtp_type}")
        logger.info(f"Username: {creds.username}")
    except Exception as e:
        logger.error(f"Failed to load email credentials: {e}")
        return False

    if body is None:
        body = f"""
This is a test email from Prefect.

Sent at: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Email block: {email_block_name}
SMTP Server: {creds.smtp_server}
SMTP Port: {creds.smtp_port}
SMTP Type: {creds.smtp_type}

If you received this email, your SMTP configuration is working correctly.
"""

    return send_email(
        recipients=[to],
        subject=subject,
        body=body,
        email_block_name=email_block_name,
    )


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m flows.email <to_address> [subject] [body]")
        print()
        print("Examples:")
        print("  python -m flows.email test@example.com")
        print('  python -m flows.email test@example.com "My Subject"')
        print('  python -m flows.email test@example.com "My Subject" "My body text"')
        sys.exit(1)

    to_address = sys.argv[1]
    subject = sys.argv[2] if len(sys.argv) > 2 else "Test Email from Prefect"
    body = sys.argv[3] if len(sys.argv) > 3 else None

    send_test_email(to=to_address, subject=subject, body=body)