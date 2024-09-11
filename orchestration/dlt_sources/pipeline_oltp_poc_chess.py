# Seeds our OLTP database from the chess API.  We use postgres, but any SQL DB DLT supports can be used by changing the
# destination functions.  We do this outside of dagster because it currently does not support chaining of DLT pipelines.
# It's also worth noting that chess.com will govern you if you run this seed too many times in one day, so caveat emptor.

import os

from dlt import pipeline
from dlt.destinations import postgres

from chess import source

# We wire this up manually so that we can support multiple sources of the same type
group_name              = "oltp"
db_name                 = os.getenv("SOURCES__POC_DB__CREDENTIALS__DATABASE")
schema_name             = os.getenv("SOURCES__POC_DB__CREDENTIALS__SCHEMA")

source_name             = "chess_api"

destination_name        = os.getenv("SOURCES__POC_DB__CREDENTIALS__DRIVERNAME")
destination_username    = os.getenv("SOURCES__POC_DB__CREDENTIALS__USERNAME")
destination_password    = os.getenv("SOURCES__POC_DB__CREDENTIALS__PASSWORD")
destination_host        = os.getenv("SOURCES__POC_DB__CREDENTIALS__HOST")
destination_port        = os.getenv("SOURCES__POC_DB__CREDENTIALS__PORT")
destination_credentials = f"{destination_name}://{destination_username}:{destination_password}@{destination_host}:{destination_port}/{db_name}"
destination             = postgres(credentials=destination_credentials)

dlt_source = source(
    players=[
        "MagnusCarlsen",
        "Hikaru",
        "bogdandeac",
        "Prizant_academy",
        "lachesisq",
        # "HansOnTwitch",
        # "firouzja2003",
        # "fabianocaruana",
        # "nihalsarin",
        # "mishanick",
        # "mishanick",
        # "fairchess_on_youtube",
        # "baku_boulevard",
        # "anishgiri",
        # "vi_pranav",
        # "oleksandr_bortnyk",
    ],
    start_month="2024/01",
    end_month="2024/08",
)

dlt_pipeline = pipeline(
    pipeline_name=f"{db_name}__{schema_name}__{group_name}",
    dataset_name=schema_name,
    destination=destination,
    progress="log",
)

result = dlt_pipeline.run(dlt_source.with_resources("players_profiles", "players_games"))
print(result)
