# Airflow Callbacks

Showcasing several ways to implement Airflow callbacks and notifications via Slack.

## Description

Monitoring tasks and DAGs at scale can be cumbersome. Sometimes you'd like to be notified of certain events, and not others. These DAGs cover several methods of implementing custom Slack notifications, so you can be confident you aren't missing critical events that may require immediate attention.

## Slack Callback Examples
![Example Callbacks](https://github.com/astronomer/customer-success-labs/blob/main/airflow-callbacks/docs/images/notifications.png)

## Getting Started

### Dependencies

To implement notifications via Slack, add the following to your requirements.txt:
```
apache-airflow-providers-slack
apache-airflow-providers-http
```

### Installing

In order to run these demos on your localhost, be sure to install:

* [Docker](https://www.docker.com/products/docker-desktop)

* [Astro CLI](https://docs.astronomer.io/astro/install-cli)


### Executing demos

Clone this repository, then navigate to the ```cs-tutorial-slack-callbacks``` directory and start your local Airflow instance:
```
astrocloud dev start
```

In your browser, navigate to ```http://localhost:8080/```

* Username: ```admin```

* Password: ```admin```


### Setting up Slack Connections in Airflow
In order to receive callback notifications, you must also create your webhooks and set up your connections in the Airflow UI. Follow the instructions found in the [Appendix section](https://docs.google.com/presentation/d/1lnu3IfM82I09yK7XuzGcroDNMlZpqs-3nARDCWpfaDI/edit#slide=id.ge7d1e4d78d_2_3) of the accompanying slide deck to create these.


## Additional Resources

* [Notifications Overview Slides](https://docs.google.com/presentation/d/1lnu3IfM82I09yK7XuzGcroDNMlZpqs-3nARDCWpfaDI/edit?usp=sharing)
* [Astronomer Guide - Error Notifications in Airflow](https://www.astronomer.io/guides/error-notifications-in-airflow)
* [Astronomer Webinar - Monitor Your DAGs with Airflow Notifications](https://www.astronomer.io/events/webinars/dags-with-airflow-notifications/)
* [Configure Airflow Email Alerts on Astronomer](https://docs.astronomer.io/astro/airflow-alerts/#configure-airflow-email-alerts)
