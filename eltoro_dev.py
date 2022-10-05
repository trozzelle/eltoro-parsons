import os
import json
import logging
from datetime import datetime
from datetime import date
from functools import reduce
from dotenv import load_dotenv
from parsons import Table, eltoro, Postgres


logger = logging.getLogger(__name__)
_handler = logging.StreamHandler()
_formatter = logging.Formatter("{levelname} {message}", style="{")
_handler.setFormatter(_formatter)
logger.addHandler(_handler)
logger.setLevel("INFO")

load_dotenv()

current_time = datetime.now().strftime("%H:%M:%S")
current_date = date.today().strftime("%Y-%m-%d")


class ETData:
    et_connector = None
    endpoint_key = None

    def __init__(self, et_connector=None, start_date=None, stop_date=None):

        self.et_connector = et_connector

        self.summary = {}
        self.performance = {}

        self.start_date = start_date or date(date.today().year, 1, 1)
        self.stop_date = stop_date or date.today().strftime("%Y-%m-%d")

    def change_case(self, str):
        return reduce(lambda x, y: x + ("_" if y.isupper() else "") + y, str).lower()

    def write_to_db(self, data=None, conn=None, db=None, write_method="fail"):

        data.add_column("date_pulled", date.today())

        df = data.to_dataframe()

        try:
            conn.copy(data, db, if_exists=write_method)
            logging.info(f"Write succeeded.")

        except Exception as err:
            logging.error(f"Write failed with: {err}")

    def get_summary(self, endpoint):
        pass

    def write_summary(self, conn):
        pass

    def prepare_data(self, data):
        pass

    def get_performance(self, granularity):

        granularity = granularity or self.granularity

        logger.info("Pulling campaigns performance stats...")

        # Get list of unique IDs
        ids = list(set(self.summary["id"]))

        stats_all = []

        # Iterate over ID list and make request from 'stats' endpoint
        for id in ids:
            params = {f"{self.endpoint_key}": f"{id}"}
            stats_single = self.et_connector.get_stats(
                granularity=granularity,
                start=self.start_date,
                stop=self.stop_date,
                **params,
            )
            stats_single.add_column("id", id, index=0)
            stats_single.remove_column("start", "end")
            stats_all.append(stats_single)

        # Combine responses into single Parsons table
        performance = Table([])
        performance.concat(*stats_all)

        logger.info("Done.")

        self.performance = performance
        return self.performance

    def write_performance(self):
        pass


class Organizations(ETData):
    endpoint = "orgs"

    def __init__(self, et_connector=None, start_date=None, stop_date=None):
        super().__init__(et_connector, start_date, stop_date)

        self.endpoint_key = "orgId"

    def get_summary(self):
        data = self.et_connector.get_organizations()

        self.summary = self.prepare_data(data)
        return self.summary

    def write_summary(self, conn=None, db=None, write_method="fail"):
        db = db or f"eltoro.{self.endpoint}"

        logger.info(f"Writing {self.endpoint} to {db}")
        super().write_to_db(
            data=self.summary, conn=conn, db=db, write_method=write_method
        )

    def prepare_data(self, table):
        logger.info("Cleaning organizations data...")

        # Rename default columns and remove ones we know we don't want to store
        table.rename_column("_id", "id")
        table.remove_column(
            "accountinghold",
            "minTotalImpressionsOverride",
            "contactSales",
            "accountReps",
            "billCycle",
            "parents",
            "buckets",
        )

        # Unpack account notes, stats, and contact columns
        table.unpack_dict("conf", keys=["notes"], prepend=False)
        table.unpack_dict("stats", prepend=False)
        table.unpack_dict("contactOps", prepend_value="contact")

        # Unpack the nested columns we just unpacked
        table.unpack_dict("campaigns", prepend_value="campaigns")
        table.unpack_dict("orderLines", prepend_value="orderLines")
        table.unpack_dict("creatives", prepend_value="creatives")
        table.unpack_dict("buckets", prepend_value="buckets")
        table.unpack_dict("users", prepend_value="users")
        table.unpack_dict("orgs", prepend_value="orgs")

        # CamelCase to snake_case
        column_map = {self.change_case(column): [column] for column in table.columns}
        table = table.map_and_coalesce_columns(column_map)

        logger.info("Done.")

        return table

    def get_performance(self, granularity=None):
        logger.info("Pulling organization performance stats...")

        # Pull organization performance stats
        performance = et.get_stats(granularity="day", orgId=ORGANIZATION_ID)

        # organization_performance = iterate_performance(['test'], PULL_START_DATE, PULL_STOP_DATE, identifier="orgId")
        performance.add_column("org_id", ORGANIZATION_ID, index=0)

        # Remove extraneous start, end columns.
        performance.remove_column("start", "end")
        logger.info("Done.")

        self.performance = performance

        return self.performance

    def write_performance(self, conn=None, db=None, write_method="fail"):
        db = db or f"eltoro.{self.endpoint}_performance"

        logger.info(f"Writing {self.endpoint} performance to {db}")
        super().write_to_db(
            data=self.performance, conn=conn, db=db, write_method=write_method
        )


class Campaigns(ETData):
    endpoint = "campaigns"

    def __init__(self, et_connector=None, start_date=None, stop_date=None):
        super().__init__(et_connector, start_date, stop_date)

        self.endpoint_key = "campaignId"

    def get_summary(self):
        data = self.et_connector.get_campaigns()

        self.summary = self.prepare_data(data)
        return self.summary

    def write_summary(self, conn=None, db=None, write_method="fail"):
        db = db or f"eltoro.{self.endpoint}"

        logger.info(f"Writing {self.endpoint} to {db}")
        super().write_to_db(
            data=self.summary, conn=conn, db=db, write_method=write_method
        )

    def prepare_data(self, table):
        logger.info("Cleaning campaigns data...")

        # Rename default columns and remove ones we know we don't want to store
        table.rename_column("_id", "id")
        table.remove_column("org", "orgIdParents")
        table.rename_column("thumb", "thumbnail")

        # Unpack orderline performance statistics
        table.unpack_dict(column="stats", prepend=False)

        # CamelCase to snake_case
        column_map = {self.change_case(column): [column] for column in table.columns}
        table = table.map_and_coalesce_columns(column_map)

        # Fix overzealous case
        table.rename_column("served_c_t_r", "served_ctr")

        logger.info("Done.")

        return table

    def get_performance(self, granularity=None):
        super().get_performance(granularity)
        return self.performance

    def write_performance(self, conn=None, db=None, write_method="fail"):
        db = db or f"eltoro.{self.endpoint}_performance"

        logger.info(f"Writing {self.endpoint} performance to {db}")
        super().write_to_db(
            data=self.performance, conn=conn, db=db, write_method=write_method
        )


class Orderlines(ETData):
    endpoint = "orderlines"

    def __init__(self, et_connector=None, start_date=None, stop_date=None):
        super().__init__(et_connector, start_date, stop_date)

        self.endpoint_key = "orderLineId"

    def get_summary(self):
        data = self.et_connector.get_orderlines()

        self.summary = self.prepare_data(data)
        return self.summary

    def write_summary(self, conn=None, db=None, write_method="fail"):
        db = db or f"eltoro.{self.endpoint}"

        logger.info(f"Writing {self.endpoint} to {db}")
        super().write_to_db(
            data=self.summary, conn=conn, db=db, write_method=write_method
        )

    def prepare_data(self, table):
        logger.info("Cleaning table data...")

        # Rename default columns and remove ones we know we don't want to store
        table.rename_column("_id", "id")
        table.rename_column("thumb", "thumbnail")
        table.remove_column(
            "minTotalImpressionsOverride",
            "statesTouched",
            "userIds",
            "priorityAudit",
            "deployEmailSent",
            "progressSteps",
            "org",
            "orgIdParents",
            "user",
            "campaign",
            "alternative_landing_page_url",
            "politicalOrderLine",
            "creatives",
            "reviews",
            "strategy",
            "bucketIdsDetached",
            "creativeIdsDetached",
            "locked",
            "updated",
        )

        # Unpack orderline performance statistics
        table.unpack_dict("stats", prepend=False)

        # Remove unpacked stats we don't want to store
        table.remove_column(
            "matchRatePrivate", "matchedTargetsPrivate", "totalTargetsPrivate"
        )

        # CamelCase to snake_case
        column_map = {self.change_case(column): [column] for column in table.columns}
        table = table.map_and_coalesce_columns(column_map)

        # Fix overzealous case
        table.rename_column("served_c_t_r", "served_ctr")

        # Convert columns we want to store as type jsonb in Postgres to json strings, otherwise
        # psycopg2 tries to load as varchar
        table.convert_column(
            ["buckets", "political_transparency_data", "served_daily"],
            lambda x: json.dumps(x) if x is not None else None,
        )

        logger.info("Done.")

        return table

    def get_performance(self, granularity=None):
        super().get_performance(granularity)
        return self.performance

    def write_performance(self, conn=None, db=None, write_method="fail"):
        db = db or f"eltoro.{self.endpoint}_performance"

        logger.info(f"Writing {self.endpoint} performance to {db}")
        super().write_to_db(
            data=self.performance, conn=conn, db=db, write_method=write_method
        )


class Creatives(ETData):
    endpoint = "creatives"

    def __init__(self, et_connector=None, start_date=None, stop_date=None):
        super().__init__(et_connector, start_date, stop_date)

        self.endpoint_key = "creativeId"

    def get_summary(self):
        data = self.et_connector.get_creatives()

        self.summary = self.prepare_data(data)
        return self.summary

    def write_summary(self, conn=None, db=None, write_method="fail"):
        db = db or f"eltoro.{self.endpoint}"

        logger.info(f"Writing {self.endpoint} to {db}")
        super().write_to_db(
            data=self.summary, conn=conn, db=db, write_method=write_method
        )

    def prepare_data(self, table):
        logger.info("Cleaning creatives data...")

        # Rename default columns and remove ones we know we don't want to store
        table.rename_column("_id", "id")
        table.rename_column("thumb", "thumbnail")
        table.remove_column("error", "errorData")
        table.remove_column("org", "orgIdParents", "sentForMiscBilling")

        # CamelCase to snake_case
        column_map = {self.change_case(column): [column] for column in table.columns}
        table = table.map_and_coalesce_columns(column_map)

        # eltoro's API sends empty lists. Replace with null if empty
        table.convert_column("order_lines", lambda x: x if len(x) > 0 else None)

        # Convert columns we want to store as type jsonb in Postgres to json strings, otherwise
        # psycopg2 tries to load as varchar
        table.convert_column(
            ["files", "order_lines"], lambda x: json.dumps(x) if x is not None else None
        )

        logger.info("Done.")

        return table

    def get_performance(self, granularity=None):
        super().get_performance(granularity)
        return self.performance

    def write_performance(self, conn=None, db=None, write_method="fail"):
        db = db or f"eltoro.{self.endpoint}_performance"

        logger.info(f"Writing {self.endpoint} performance to {db}")
        super().write_to_db(
            data=self.performance, conn=conn, db=db, write_method=write_method
        )


class Buckets(ETData):
    endpoint = "buckets"

    def __init__(self, et_connector=None, start_date=None, stop_date=None):
        super().__init__(et_connector, start_date, stop_date)

        self.endpoint_key = None

    def get_summary(self):
        data = self.et_connector.get_buckets()

        self.summary = self.prepare_data(data)
        return self.summary

    def write_summary(self, conn=None, db=None, write_method="fail"):
        db = db or f"eltoro.{self.endpoint}"

        logger.info(f"Writing {self.endpoint} to {db}")
        super().write_to_db(
            data=self.summary, conn=conn, db=db, write_method=write_method
        )

    def prepare_data(self, table):
        logger.info("Cleaning buckets data...")

        table.rename_column("_id", "id")

        # Remove extraneous columns. 'columns', 'payload', and 'quoteColumns' have the schema of
        # uploaded target lists and sampled data from targets
        table.remove_column("columns", "payload", "org", "orgIdParents", "quoteColumns")
        table.unpack_dict("stats", prepend_value="targets")

        # The contents of 'conf' depend on the bucket type. We grab fields related to bucket
        # hierarchy or and the FeatureCollections
        table.unpack_dict(
            "conf",
            keys=["parentBucketId", "parentId", "geoframe", "map", "buckets"],
            include_original=True,
            prepend=False,
        )

        # Unpack another level down.
        table.unpack_dict("geoframe", prepend_value="g")
        table.unpack_dict("g_timeframes", prepend_value="timeframe")
        table.rename_column("g_requested", "requested")
        table.remove_column("g_dense_polygon_size")

        # Coalesce map columns into one. Contents are FeatureCollections
        table = table.map_and_coalesce_columns({"map": ["g_map"]})

        # CamelCase to snake_case
        column_map = {self.change_case(column): [column] for column in table.columns}
        table = table.map_and_coalesce_columns(column_map)

        # Convert columns we want to store as type jsonb in Postgres to json strings, otherwise
        # psycopg2 tries to load as varchar
        table.convert_column(
            [
                "conf",
                "files",
                "order_lines",
                "deploy",
                "buckets",
                "map",
            ],
            lambda x: json.dumps(x) if x is not None else None,
        )

        logger.info("Done.")

        return table

    def get_performance(self, granularity=None):
        # super().get_performance(granularity)
        # return self.performance

        print("NOT YET IMPLEMENTED")

    def write_performance(self, conn=None, db=None, write_method="fail"):
        db = db or f"eltoro.{self.endpoint}_performance"

        logger.info(f"Writing {self.endpoint} performance to {db}")
        super().write_to_db(
            data=self.performance, conn=conn, db=db, write_method=write_method
        )


if __name__ == "__main__":

    API_USER = os.getenv("ELTORO_API_USER")
    API_PASSWORD = os.getenv("ELTORO_API_PASSWORD")

    DB_USER = os.getenv("NX_DB_USER")
    DB_PASS = os.getenv("NX_DB_PASS")
    DB_NAME = os.getenv("NX_DB_NAME")
    DB_HOST = os.getenv("NX_DB_HOST")
    DB_PORT = os.getenv("NX_DB_PORT")

    et = eltoro.Eltoro(API_USER, API_PASSWORD, production=True)

    ## Quick defaults. Change this to your OrgID
    ORGANIZATION_ID = "99sEzsHh7HXN4pQ9R"
    PULL_START_DATE = "2022-01-01"
    PULL_STOP_DATE = date.today().strftime("%Y-%m-%d")

    logger.info(
        f"Starting pull for {current_date} at {current_time}\n\nPull starts on {PULL_START_DATE} and ends on "
        f"{PULL_STOP_DATE}"
    )

    pg = Postgres(
        username=DB_USER, password=DB_PASS, host=DB_HOST, db=DB_NAME, port=DB_PORT
    )

    organizations = Organizations(et)
    organizations.get_summary()
    organizations.write_summary(pg, write_method="truncate")
    organizations.get_performance(granularity="day")
    organizations.write_performance(pg, write_method="truncate")

    campaigns = Campaigns(et)
    campaigns.get_summary()
    campaigns.write_summary(pg, write_method="truncate")
    campaigns.get_performance(granularity="day")
    campaigns.write_performance(pg, write_method="truncate")

    orderlines = Orderlines(et)
    orderlines.get_summary()
    orderlines.write_summary(pg, write_method="truncate")
    orderlines.get_performance(granularity="day")
    orderlines.write_performance(pg, write_method="truncate")

    creatives = Creatives(et)
    creatives.get_summary()
    creatives.write_summary(pg, write_method="truncate")
    creatives.get_performance(granularity="day")
    creatives.write_performance(pg, write_method="truncate")

    buckets = Buckets(et)
    buckets.get_summary()
    buckets.write_summary(pg, write_method="truncate")
