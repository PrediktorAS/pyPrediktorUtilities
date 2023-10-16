from email_validator import validate_email, EmailNotValidError
from pydantic import AnyUrl, validate_call, EmailStr
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import os

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class SendEmail:
    """Helper function to send emails with attachments using SMTP
    
    Args:
        server (AnyUrl): SMTP server address
        port (int): SMTP server port
        username (str): SMTP server username
        password (str): SMTP server password
        
    Returns:
        Object: SendEmail object
    """
    
    @validate_call
    def __init__(self, server: AnyUrl, port: int, username: str, password: str) -> object:
        """Class initialiser

        Args:
            server (AnyUrl): _description_
            port (int): _description_
            username (str): _description_
            password (str): _description_

        Returns:
            object: SendEmail object
        """
        self.smtp_server = server
        self.smtp_server_port = port
        self.smtp_server_username = username
        self.smtp_server_password = password

    @validate_call
    def send_email(self, from_email: EmailStr, recipients: [EmailStr], subject: str, body: str, files: list = []):
        """_summary_

        Args:
            from_email (EmailStr): The sender email address
            recipients (list of EmailStr): list of one or more recipient email addresses
            subject (str): The email subject
            body (str): The email body
            files (list, optional): A list of paths to files to include. Defaults to [].
        """
        msg = MIMEMultipart()
        msg["From"] = from_email
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        for file in files:
            with open(file, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header(
                    "Content-Disposition",
                    f"attachment; filename={os.path.basename(file)}",
                )
                msg.attach(part)

        for recipient in recipients:
            msg["To"] = recipient
            text = msg.as_string()
            with smtplib.SMTP(self.smtp_server, self.smtp_server_port) as server:
                server.starttls()
                server.login(self.smtp_server_username, self.smtp_server_password)
                server.sendmail(self.smtp_server_username, recipient, text)
                logging.info(f"Email sent to {recipient} with subject {subject} and {len(files)} attachments")


if __name__ == "__main__":
    pass
