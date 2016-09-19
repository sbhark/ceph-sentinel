#!/usr/bin/python

# Imports
from datetime import datetime
from random import randint

import logging as logger
import subprocess
import time
import mandrill
import json
import sys

# Global Script variable
ceph_io_log = []
mandrill_api_key = ""
email_subject = " Ceph Sentinel"
from_email = ""
to_email = ""
sentinel_data_file = "/home/sentinel_data.json"
cluster_location = "LAB"

# Configure logger
logger.basicConfig(filename="/tmp/reboot_osd.log", level=logger.INFO)

# Get Client IO data from Ceph via ceph -s
# Returns a list of 10 io data points 
def get_client_io():
    # Query ceph -s to get client i/o
    logger.info("Executing command: ceph -s")
    ceph_query = subprocess.Popen("ceph -s", 
                    stdout=subprocess.PIPE, shell=True)
    (output, error) = ceph_query.communicate()

    # If there was an error executing the command, just 
    # return 0 for client io. 
    if error: 
        logger.info("Error executing command: ceph -s")
        return 0

    # Get the output as lines in a list 
    # Last element is the client i/o 
    ceph_status = output.splitlines()

    ceph_client_io = None
    for line in ceph_status:
        if "client io" in line:
            logger.info("Detected client io: " + line)
            ceph_io_log.append(line)
            ceph_client_io = line

    if not ceph_client_io:
        logger.info("No client io detected")
        ceph_io_log.append("No client io detected")
        return 0

    # Perform string operations to extract the io number
    ceph_client_ops = ceph_client_io.split(',')[1]
    ceph_client_ops_number = int(ceph_client_ops.split()[0])

    return ceph_client_ops_number

# Has logic to determine if an osd requires a reboot
def determine_reboot():
    client_io_list = []
    reboot_threshold = 7
    valid_no_io = 9
    valid_no_io_threshold = 3

    # Get 10 data points for client io,
    # Sleep for 2 seconds in each collection so we get a more 
    # distributed range of data. 
    for i in range(0, 10):
        client_io = get_client_io()
        client_io_list.append(client_io)
        time.sleep(2)

    # Go through the data points and increment the reboot_count
    # by 1 if the data point is of number 0
    reboot_count = 0 
    for io in client_io_list:
        if io == 0:
            reboot_count += 1

    logger.info("Reboot count is: " + str(reboot_count))
    
    # Determine if no client io means that there is no data being
    # passed from the repository server.
    if reboot_count == valid_no_io:
        logger.info("No client io detected from repository server")
        # Load JSON data file
        sentinel_data = open(sentinel_data_file).read()
        sentinel_data_json = json.loads(sentinel_data)

        # Check if valid no io counter is reached
        if sentinel_data_json["no_client_io_count"] == valid_no_io_threshold:
            # Reset counter to 0 
            logger.info("Resetting database no client io count")
            sentinel_data_json["no_client_io_count"] = 0
            with open(sentinel_data_file, "w") as sentinel_file:
                json.dump(sentinel_data_json, sentinel_file)
            
            logger.info("Not rebooting OSD as sentinel detected valid no io")
            logger.info("Sending email notification of no client io")
            email_message = ("No client IO detected \n")
            for io_log in ceph_io_log:
                email_message += ("\n " + io_log + " \n")

            send_notification("INFO NO Client IO - " + cluster_location + 
                email_subject, email_message)
        else:
            # Increment counter 
            sentinel_data_json["no_client_io_count"] += 1
            logger.info("Updating database with no client io count")
            with open(sentinel_data_file, "w") as sentinel_file:
                json.dump(sentinel_data_json, sentinel_file)

            # Poll cluster again recursively
            determine_reboot()

        return None

    # If reboot count number is greater or = reboot_threshold 
    # then return True to initiate an OSD reboot
    if reboot_count >= reboot_threshold:
        logger.info("Reboot threshold triggered, an OSD requires reboot")
        return True
    else:
        logger.info("Cluster healthy, currently accepting client IO")
        return False

# Send command to initiate a random OSD reboot
def reboot_random_osd():
    # These variables need to be configured per OSD host
    # and what OSDs it is handling
    host_osd_min = 4
    host_osd_max = 9

    random_osd = str(randint(host_osd_min, host_osd_max))

    logger.info("Restarting OSD: " + random_osd + " OSD")
    restart = subprocess.Popen("/etc/init.d/ceph restart osd." + random_osd,
                    stdout=subprocess.PIPE, shell=True)
    (output, error) = restart.communicate()

    # If there was an error executing the command, just 
    # return 0 for client io.
    email_message = ""
    if error:
        logger.info("Error restarting OSD: " + random_osd)
        email_message = ("Ceph cluster is UNHEALTHY, OSD reboot required\n")
        email_message += ("Failed to reboot OSD: " + random_osd + "\n")
        for io_log in ceph_io_log:
            email_message += ("\n " + io_log + " \n")

        send_notification("WARNING - " + cluster_location + 
            email_subject, email_message)
        return False

    logger.info("Successfully restarted OSD: " + random_osd)

    email_message = ("Ceph cluster is UNHEALTHY, OSD reboot required\n ")
    email_message += ("Successfully restarted OSD: " + random_osd)
    for io_log in ceph_io_log:
        email_message += ("\n " + io_log + " \n")

    send_notification("WARNING - " + cluster_location + 
        email_subject, email_message)

    return True

# Send notification email to specified recipients
def send_notification(subject, message_content):'
    try:
        logger.info("Sending notification via Mandrill")
        mandrill_client = mandrill.Mandrill(mandrill_api_key)
        message = {
                'from_email': from_email,
                'subject': subject,
                'text': message_content,
                'to': [
                    {'email': to_email,
                    'type': 'to'}]
            }
        message_sent = mandrill_client.messages.send(message=message)
        if message_sent:
            logger.info("Successfully sent notification via Mandrill")
        else:
            logger.info("Failed to send notification via Mandrill")
            logger.info(str(message_sent))

    except mandrill.Error, e:
        logger.info("Failed to send notification via Mandrill")
        logger.info("Mandrill error detected" + str(e))

# Main method to execute this script
if __name__=="__main__":
    logger.info("--- Start: Slap Ceph in the Face ---")
    logger.info("Time now is: " + str(datetime.now()))

    reboot_required = determine_reboot()

    if reboot_required is None:
        logger.info("Ceph cluster has no client io")
        sys.exit(0) 

    email_message = ""
    if reboot_required:
        logger.info("Ceph cluster has no IO, OSD reboot required")
        reboot_random_osd()
    else:
        email_message = ("Ceph cluster is healthy, no OSD reboot required")
        logger.info(email_message)
        for io_log in ceph_io_log:
            email_message += ("\n " + io_log + " \n")

        send_notification("HEALTHY - " + cluster_location 
            + email_subject, email_message)

    logger.info("--- End: Slap Ceph in the Face ---")

