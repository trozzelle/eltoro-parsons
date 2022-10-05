import requests
import json
from datetime import date, timedelta
from parsons.etl import Table
from parsons.utilities import check_env
from parsons.utilities.api_connector import APIConnector
import logging

logger = logging.getLogger(__name__)


class Eltoro:

    def __init__(self,
                 api_user=None,
                 api_password=None,
                 api_token=None,
                 production=False):
        """

        Args:
            api_user: str
                The eltoro api user. Not required if ``ELTORO_API_USER`` env variable is
                passed.
            api_password: str
                The eltoro api password. Not required if ``ELTORO_API_PASSWORD`` env variable is
                passed.
            api_token: str
                A valid eltoro api authentication token. Not required if ``ELTORO_API_TOKEN`` env variable is
                passed. If not provided, the init method will use the api_user and api_password to retrieve a new
                token.
            production: bool
                A flag to set whether to call the production API vs the development sandbox. Defaults to the development
                sandbox. Actions in the production api may be billable, though that mostly pertains to PUT and UPDATE
                calls.
        """
        self.token = check_env.check('ELTORO_API_TOKEN', api_token)

        ## By default, make calls to sandbox environment.
        if production:
            self.uri = "https://api-prod.eltoro.com/"
        else:
            self.uri = "https://api-sandbox.eltoro.com/"

        self.api_user = check_env.check("ELTORO_API_USER", api_user)
        self.api_password = check_env.check("ELTORO_API_PASSWORD", api_password)

        self.current_date = date.today()

        headers = {
            "Content-Type": "application/json",
        }

        ## If a valid token is not provided, use api credentials to request new token and store.
        if self.token == None:
            logger.info("No token provided. Authenticating with API to get valid token.")

            ## Set auth request payload
            auth_payload = json.dumps(
                {"password": f"{api_password}", "username": f"{api_user}"}
            )

            ## Authenticate with eltoro, add bearer token to header
            response = requests.request(
                "POST", f"{self.uri}users/login", headers=headers, data=auth_payload
            ).json()
            self.token = response["token"]
            headers["Authorization"] = f"Bearer {self.token}"

            logger.info(
                f"Successfully authenticated with {self.uri}.\n\nAuthentication token added to header."
            )

        ## Instantiate connector class
        self.client = APIConnector(self.uri, headers=headers)
        logger.info(f"Instantiated eltoro client.")

    def _paginated_request(self,
                           endpoint,
                           params=None):

        """
        Internal method that handles making the call and paging through the results.

        Args:
            endpoint: str
                API endpoint to call.
            params: dict
                Extra parameters to send.

        Returns:
            List of items in the full response.
        """

        response = self.client.get_request(f"{self.uri}{endpoint}", params=params)

        total_results = response["paging"]["total"]
        current_page = response["paging"]["page"]
        total_pages = response["paging"]["pages"]

        if response["paging"]["page"] == response["paging"]["pages"]:

            logger.info(
                f"Request to {endpoint} succeeded. Grabbed {total_results} results."
            )
            return response["results"]

        else:

            logger.info(
                f"Request to {endpoint} succeeded. Paginating through {total_results} results..."
            )
            paginated_response = response["results"]

            next_page = response["paging"]["page"] + 1
            params["pagingPage"] = params["pagingPage"] or 1

            while params["pagingPage"] <= response["paging"]["pages"]:
                response = self.client.get_request(
                    f"{self.uri}{endpoint}", params=params
                )

                logger.info(f"Page {params['pagingPage']} of {total_pages}")

                paginated_response += response["results"]
                params["pagingPage"] += 1

                next_page += 1

            return paginated_response

    def get_campaigns(
            self,
            search=None,
            includeChildren=None,
            orgId=None,
            name=None,
            refId=None,
            status=None,
            expiration=None,
            pagingLimit=None,
            pagingPage=None,
            pagingSortCol=None,
            pagingSortAsc=None,
    ):

        """
        Get campaigns data. No parameters will grab all campaigns associated
        with the user's primary organization.

        Args:
            search: str
                Search by name, id, or organization. Allows partial values.
            includeChildren: str
                Include campaigns from child organizations.
            orgId: str
                Organization from which to return campaigns.
            name: str
                Name of campaign.
            refId: str
                Specific campaign ID to return.
            status: str
                Specify deployment status.
            expiration: str
                Specify expiration status.
            pagingLimit: str
                Limit returned results to x pages, returning 50 results per page.
            pagingPage: str
                Return results from a specific page.
            pagingSortCol: str
                Column to sort by.
            pagingSortAsc: str
                Whether to sort ascending or descending.

        Returns:
            Parsons table of campaign data

        """

        params = {
            "search": search,
            "includeChildren": includeChildren,
            "orgId": orgId,
            "name": name,
            "refId": refId,
            "status": status,
            "expiration": expiration,
            "pagingLimit": pagingLimit,
            "pagingPage": pagingPage,
            "pagingSortCol": pagingSortCol,
            "pagingSortAsc": pagingSortAsc,
        }

        logger.info("Calling campaigns endpoint.")

        response = self._paginated_request("campaigns", params=params)

        tbl = Table(response)
        logger.info(f"{tbl.num_rows} results.")

        if tbl.num_rows > 0:
            return tbl
        else:
            return Table()

    def get_orderline(self,
                      id=None):
        """
        Calls orderlines endpoint for a single orderliine's data.

        Args:
            id: str
        Returns:
            Parsons table of a single orderline's stats.
        """
        params = {
            "id": id,
        }

        response = self.client.get_request(f"orderLines/{id}")

        tbl = Table([response])

        if tbl.num_rows > 0:
            return tbl
        else:
            return Table()

    def get_orderlines(
            self,
            search=None,
            includeChildren=None,
            orgId=None,
            name=None,
            campaignId=None,
            targetType=None,
            creativeType=None,
            status=None,
            phase=None,
            statusAny=None,
            targetTypeAny=None,
            creativeTypeAny=None,
            pagingLimit=None,
            pagingPage=None,
            pagingSortCol=None,
            pagingSortAsc=None,
    ):
        """
        Get orderlines data. No parameters will grab all orderlines associated
        with the user's primary organization.

        Args:
            search: str
                Search by name, id, or organization. Allows partial values.
            includeChildren: str
                Include orderlines from child organizations.
            orgId: str
                Organization from which to return orderlines.
            name: str
                Name of orderline.
            campaignId: str
                Specific campaign ID from return orderlines.
            targetType: str
                Specify target type.
            creativeType: str
                Specify creative type.
            status: str
                Specify deployment status.
            phase: str
                Specify the orderline deployment phase.
            statusAny: str
                Returns orderlines of multiple statuses. Accepts list of statuses.
            targetTypeAny: str
                Returns orderlines of multiple target types. Accepts list of target types.
            creativeTypeAny: str
                Returns orderlines of multiple creative types. Accepts list of creative types.
            pagingLimit: str
                Limit returned results to x pages, returning 50 results per page.
            pagingPage: str
                Return results from a specific page.
            pagingSortCol: str
                Column to sort by.
            pagingSortAsc: str
                Whether to sort ascending or descending.

        Returns:
            Parsons table of orderlines data

        """
        params = {
            "search": search,
            "includeChildren": includeChildren,
            "orgId": orgId,
            "name": name,
            "campaignId": campaignId,
            "targetType": targetType,
            "creativeType": creativeType,
            "status": status,
            "phase": phase,
            "statusAny": statusAny,
            "targetTypeAny": targetTypeAny,
            "creativeTypeAny": creativeTypeAny,
            "pagingLimit": pagingLimit,
            "pagingPage": pagingPage,
            "pagingSortCol": pagingSortCol,
            "pagingSortAsc": pagingSortAsc,
        }

        logger.info("Calling orderlines endpoint.")

        response = self._paginated_request("orderLines", params=params)

        tbl = Table(response)
        logger.info(f"{tbl.num_rows} results.")

        if tbl.num_rows > 0:
            return tbl
        else:
            return Table()

    def get_organizations(
            self,
            search=None,
            includeChildren=None,
            orgId=None,
            name=None,
            pac=None,
            pagingLimit=None,
            pagingPage=None,
            pagingSortCol=None,
            pagingSortAsc=None,
    ):
        """
        Get organization data. No parameters will grab all organizations associated
        with the api user account.

        Args:
            search: str
                Search by name or id. Allows partial values.
            includeChildren: str
                Include child organizations.
            orgId: str
                Specific organization ID to return.
            name: str
                Specific organization name to return.
            pac: str
                Specific PAC number to return.
            pagingLimit: str
                Limit returned results to x pages, returning 50 results per page.
            pagingPage: str
                Return results from a specific page.
            pagingSortCol: str
                Column to sort by.
            pagingSortAsc: str
                Whether to sort ascending or descending.

        Returns:
            Parsons table of organization data

        """
        params = {
            "search": search,
            "includeChildren": includeChildren,
            "orgId": orgId,
            "name": name,
            "pac": pac,
            "pagingLimit": pagingLimit,
            "pagingPage": pagingPage,
            "pagingSortCol": pagingSortCol,
            "pagingSortAsc": pagingSortAsc,
        }

        response = self._paginated_request("orgs", params=params)
        tbl = Table(response)
        logger.info(f"{tbl.num_rows} results.")

        if tbl.num_rows > 0:
            return tbl
        else:
            return Table()

    def get_creatives(
            self,
            status=None,
            type=None,
            size=None,
            phase=None,
            statusAny=None,
            typeAny=None,
            pagingLimit=None,
            pagingPage=None,
            pagingSortCol=None,
            pagingSortAsc=None,
    ):
        """
        Get creatives data. No parameters will grab all creatives associated
        with the user's primary organization.

        Args:
            status: str
                Specify creative status.
            type: str
                Specify creative type.
            size: str
                Specify max size in bytes.
            phase: str
                Specify the creative deployment phase.
            statusAny: str
                Returns creatives of multiple statuses. Accepts list of statuses.
            typeAny: str
                Returns creatives of multiple types. Accepts list of types.
            pagingLimit: str
                Limit returned results to x pages, returning 50 results per page.
            pagingPage: str
                Return results from a specific page.
            pagingSortCol: str
                Column to sort by.
            pagingSortAsc: str
                Whether to sort ascending or descending.

        Returns:
            Parsons table of creatives data

        """
        params = {
            "status": status,
            "type": type,
            "size": size,
            "phase": phase,
            "statusAny": statusAny,
            "typeAny": typeAny,
            "pagingLimit": pagingLimit,
            "pagingPage": pagingPage,
            "pagingSortCol": pagingSortCol,
            "pagingSortAsc": pagingSortAsc,
        }

        logger.info("Calling creatives endpoint.")

        response = self._paginated_request("creatives", params=params)

        tbl = Table(response)
        logger.info(f"{tbl.num_rows} results.")

        if tbl.num_rows > 0:
            return tbl
        else:
            return Table()

    def get_buckets(
            self,
            search=None,
            includeChildren=None,
            orgId=None,
            userId=None,
            name=None,
            type=None,
            status=None,
            phase=None,
            statusAny=None,
            typeAny=None,
            pagingLimit=None,
            pagingPage=None,
            pagingSortCol=None,
            pagingSortAsc=None,
    ):
        """
        Get target buckets data. No parameters will grab all buckets associated
        with the user's primary organization.

        Args:
            search: str
                Search by name or id. Allows partial values.
            includeChildren: str
                Include buckets from child organizations.
            orgId: str
                Organization from which to return buckets.
            userId: str
                Specify user who creative the bucket.
            name: str
                Name of bucket.
            type: str
                Specify bucket type.
            status: str
                Specify deployment status.
            phase: str
                Specify the bucket deployment phase.
            statusAny: str
                Returns buckets of multiple statuses. Accepts list of statuses.
            typeAny: str
                Returns buckets of multiple types. Accepts list of statuses.
            pagingLimit: str
                Limit returned results to x pages, returning 50 results per page.
            pagingPage: str
                Return results from a specific page.
            pagingSortCol: str
                Column to sort by.
            pagingSortAsc: str
                Whether to sort ascending or descending.

        Returns:
            Parsons table of buckets data

        """
        params = {
            "search": search,
            "includeChildren": includeChildren,
            "orgId": orgId,
            "userId": userId,
            "name": name,
            "type": type,
            "status": status,
            "phase": phase,
            "statusAny": statusAny,
            "typeAny": typeAny,
            "pagingLimit": pagingLimit,
            "pagingPage": pagingPage,
            "pagingSortCol": pagingSortCol,
            "pagingSortAsc": pagingSortAsc,
        }

        logger.info("Calling buckets endpoint.")

        response = self._paginated_request("buckets", params=params)

        tbl = Table(response)
        logger.info(f"{tbl.num_rows} results.")

        if tbl.num_rows > 0:
            return tbl
        else:
            return Table()

    def get_stats(
            self,
            granularity="day",
            start=None,
            stop=None,
            timezone=None,
            ids=None,
            orgId=None,
            campaignId=None,
            orderLineId=None,
            creativeId=None,
            **kwargs,
    ):
        """
        Get performance statistics for a given organization, campaign, orderline, or creative at a
        specified granularity.

        Args:
            granularity: str
                'Hour', 'Day', 'Week', or 'Month'
            start: str
                Start date (inclusive)
            stop:
                Stop date (exclusive)
            timezone:
                Specify timezone (UTC if left empty)
            ids:
                Any combination of ID filters.
            orgId:
                Specific organization ID to return.
            campaignId:
                Specific campaign ID to return.
            orderLineId:
                Specific orderline ID to return.
            creativeId:
                Specific creative ID to return.
            **kwargs:

        Returns:
            Parsons table of statistics data

        """
        if not orgId and campaignId and orderLineId and creativeId:
            raise Exception(
                "At least one of orgId or campaignId or orderLineId or creativeId required"
                "to produce results."
            )

        ## If no search parameters provided
        ## set default to last 90 days by day
        stop = stop or self.current_date.strftime("%Y-%m-%d")
        start = start or (self.current_date - timedelta(60)).strftime("%Y-%m-%d")
        granularity = granularity or "day"

        params = {
            "granularity": granularity,
            "start": start,
            "stop": stop,
            "timezone": timezone,
            "ids": ids,
            "orgId": orgId,
            "campaignId": campaignId,
            "orderLineId": orderLineId,
            "creativeId": creativeId,
        }

        print_params = dict(filter(lambda item: item[1] is not None, params.items()))

        logger.info(f"Calling stats endpoint with parameters: {print(print_params)}")

        ## Stats API is not paginated
        #   Max duration
        #   Hour -> 60 days
        #   Day -> 1 year
        #   Week -> 2 years
        #   Month -> 5 years
        response = self.client.get_request("stats", params=params)
        tbl = Table(response)

        if tbl.num_rows > 0:
            return tbl
        else:
            return Table()
