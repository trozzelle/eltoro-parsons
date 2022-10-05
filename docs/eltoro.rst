Eltoro
=============

********
Overview
********

`Eltoro <https://eltoro.com/>`_ is an advertising technology provider specializing in cookieless IP targeting
and geotargeting. This class allows you to interact with the platform by leveraging their `API <https://portal.eltoro.com/docs/apidocumentation>`_.
Currently, only data pulls are implemented but the connector can be extended to support creating orderlines, uploading creatives,
and defining target buckets.

.. note::
  Authentication
   To access the API, you will need to have a user account with either the Org Admin or Read-Only role for relevant organization.

   If using Google SSO for login, it's easiest to create a separate user account just for API access.
  Sandbox Environment
   The connector defaults to the API sandbox environment at ``api-sandbox.eltoro.com``. To make calls against the production
   environment, initialize the connector with ``production=True``

***********
Quick Start
***********

To instantiate the ``Eltoro`` class, you can either pass a valid auth token or user credentials as arguments or set the
``ELTORO_API_TOKEN``, ``ELTORO_API_USER``, and ``ELTORO_API_PASSWORD`` environmental variables.

.. code-block:: python

   from parsons import Eltoro

   # Instantiate the class using environment variables
   et = Eltoro(api_user='API_USER', api_password='API_PASSWORD', production=True)

   # Get all organizations this user is a part of
   organizations = et.get_organizations()

   # Get all advocates updated in the last day
   from datetime import datetime, timedelta

   stop_date = datetime.today()
   start_date = today - datetime.timedelta(days=30)

   # get_stats pulls the granular performance data for the asset type provided
   # Here, we are providing the very first orgId in our organizations table
   performance_data = et.get_stats(granularity='Day', start=start_date.strftime('%Y-%m-%d'), stop=stop_date.strftime('%Y-%m-%d')
                  orgId=organizations['_id'][0])

***
API
***

.. autoclass :: parsons.Eltoro
   :inherited-members:
