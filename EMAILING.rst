===========================
Sending emails from Python
===========================

To make it more easy and coherent to send emails from Python, we have
created a small wrapper around the standard Python smtplib module. This
wrapper is called :class:`SendEmail` and is located can be imported into
your Python script by using the following code::

    from pyprediktorutilities.send_email import SendEmail

You'll need an SMTP server to send emails. If you don't have a specified
server in your project (e.g. from the client), you can use the generic
outbound server you'll find by searching for "SMTP" in Keeper Security.

.. note::
    If you don't have a specified server in your project (e.g. from the
    client), you can use the generic outbound server you'll find by
    searching for "SMTP" in Keeper Security. The SMTP server requires
    authentication. The username and password can be found in the Keeper
    Security.

You'll instantiate the :class:`SendEmail` class by using the following
code::

    email = SendEmail(smtp_server, smtp_port, smtp_username, smtp_password)

The only function in the :class:`SendEmail` class is the :meth:`send_email`
function. This function takes the following arguments:

* **from_email** - The email address of the sender
* **recipients** - A list of email addresses of the recipients
* **subject** - The subject of the email
* **body** - The message of the email
* **files** - A list of file paths to the attachments

The function raises an exception if the email could not be sent and will log
the results if you have defined a logger in your script.

Putting it all together, you'll have the following code:

.. code-block:: python

    from pyprediktorutilities.send_email import SendEmail

    email = SendEmail(smtp_server, smtp_port, smtp_username, smtp_password)
    email.send_email(
        "no-reply@nowhere.com",
        ["someone@somewhere.com"],
        subject, body, files)

.. note::
    If you combine this with the template engine, you can send emails
    with HTML content.