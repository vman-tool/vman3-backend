import os
from pathlib import Path

from fastapi.background import BackgroundTasks
from fastapi_mail import ConnectionConfig, FastMail, MessageSchema, MessageType

from app.shared.configs.settings import get_settings
from decouple import config

settings = get_settings()

conf = ConnectionConfig(
    MAIL_USERNAME=config("MAIL_USERNAME", default=""),
    MAIL_PASSWORD=config("MAIL_PASSWORD", default=""),
    MAIL_PORT=config("MAIL_PORT", default=1025, cast=int),
    MAIL_SERVER=config("MAIL_SERVER", default="smtp"),
    MAIL_STARTTLS=config("MAIL_STARTTLS", default=False, cast=bool),
    MAIL_SSL_TLS=config("MAIL_SSL_TLS", default=False, cast=bool),
    MAIL_DEBUG=True,
    MAIL_FROM=config("MAIL_FROM", default='noreply@test.com'),
    MAIL_FROM_NAME=config("MAIL_FROM_NAME", default=settings.APP_NAME),
    USE_CREDENTIALS=config("USE_CREDENTIALS", default=True),
    TEMPLATE_FOLDER=Path(__file__).parent.parent / "templates"
)

fm = FastMail(conf)


async def send_email(recipients: list, subject: str, context: dict, template_name: str,
                     background_tasks: BackgroundTasks):
    message = MessageSchema(
        subject=subject,
        recipients=recipients,
        template_body=context,
        subtype=MessageType.html
    )

    background_tasks.add_task(fm.send_message, message, template_name=template_name)

    background_tasks.add_task(fm.send_message, message, template_name=template_name)
