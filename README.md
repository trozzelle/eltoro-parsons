# Eltoro Parsons Connector

This repository is a connector for the [Parsons library](https://github.com/move-coop/parsons) to interact with [Eltoro's API](https://eltoro.com/).
Eltoro is an adtech provider that specializes in cookieless IP and geo-targeting. The data available through Eltoro's customer
portal is only very limited while the data provided by the REST API is extremely detailed and allows for hour-by-hour analytics.

This connector currently only supports pulling data from the various endpoints, although functions such as orderline creation, 
creatives upload, and defining target buckets are planned for the future. 


## Installation

To install manually, copy the files in `docs`, `parsons`, and `test` to the respective folders in the Parsons package.

In `parsons/__init__.py`, add `'Eltoro'` to the `__all__` list.

## Sample Script

A sample script, `eltoro_dev.py`, is provided as an example of how an organization's full data could be pulled, wrangled,
and stored in a database. It is not required for the connector to work.