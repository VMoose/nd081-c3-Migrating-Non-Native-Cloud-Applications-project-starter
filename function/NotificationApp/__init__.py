import logging
import azure.functions as func
import psycopg2
import os
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def main(msg: func.ServiceBusMessage):
    notification_id = int(msg.get_body().decode('utf-8'))
    logging.info('Python ServiceBus queue trigger processed message: %s',notification_id)
    conn = psycopg2.connect(
    host=os.environ["host"],
    database=os.environ["database"],
    user=os.environ["user"],
    password=os.environ["password"])
    # TODO: Get connection to database

    try:
        cursor = conn.cursor()
        postgreSQL_select_Query = "SELECT message, subject FROM public.notification WHERE id=" + str(notification_id)
        cursor.execute(postgreSQL_select_Query)
        notification = cursor.fetchone()
        # TODO: Get notification message and subject from database using the notification_id

        # TODO: Get attendees email and name
        postgreSQL_select_attendees_Query = "SELECT first_name, last_name, email FROM public.attendee;"
        cursor.execute(postgreSQL_select_attendees_Query)
        attendees = cursor.fetchall()
        # TODO: Loop through each attendee and send an email with a personalized subject
        
        for row in attendees:
            client = SendGridAPIClient(os.environ["SendGridAPIKey"])
            message = Mail(
                from_email=os.environ["fromMail"],
                to_emails= row[2],
                subject= notification[1],
                html_content= notification[0])
            #client.send(message)
            
        # TODO: Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        date = datetime.utcnow()
        postgreSQL_update_notification_Query = "UPDATE public.notification SET completed_date = " + "'" + str(date) + "'" + ", status=" + "'" + "Notified " + str(len(attendees)) + " attendees" + "'" + "WHERE id=" + str(notification_id)
        cursor.execute(postgreSQL_update_notification_Query)
        
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        if conn:
            conn.commit()
            cursor.close()
            conn.close()
        # TODO: Close connection
