#!/bin/bash

# exit on error
set -e

# install dbt dependencies
meltano invoke dbt deps

# try to drop the users_stream table to allow model to capture hard deleted users from source
meltano invoke dbt run-operation googleads_drop_google_ads_stream_tables || true

# run extract-load
meltano run tap-googleads "$LOADER"

# run transforms
meltano invoke dbt run -m tap_googleads
