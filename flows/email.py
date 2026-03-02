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
    logger = get_run_logger()

    if not recipients:
        logger.warning("No recipients specified, skipping email")
        return False

    # Convert paths to strings
    attachment_paths = None
    if attachments:
        attachment_paths = [str(p) for p in attachments]

    try:
        credentials = EmailServerCredentials.load(email_block_name)
        
        # Use provided email_from, or fall back to credentials username
        sender = email_from or credentials.username
        logger.info(f"Sending email from {sender} to {recipients}")

        email_send_message(
            email_server_credentials=credentials,
            subject=subject,
            msg=body,
            email_to=recipients,
            email_from=sender,
            attachments=attachment_paths,
        )

        logger.info(f"Sent email to {len(recipients)} recipients: {subject}")
        return True

    except Exception as e:
        logger.error(f"Failed to send email: {e}")
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
    return send_email(
        recipients=recipients,
        subject=subject,
        body=body,
        attachments=[filepath],
        email_block_name=email_block_name,
    )


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