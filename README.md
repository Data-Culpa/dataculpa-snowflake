# dataculpa-snowflake
Snowflake connectors for Data Culpa

This connector conforms to the [Data Culpa connector template](https://github.com/Data-Culpa/connector-template).


## Pipeline Instances

1. Clone the repo (or just ```sfdatalake.py```)
2. Install python dependencies (python3):
```
pip install python-dotenv snowflake-connector-python
```
3. Install the [Data Culpa python client](https://github.com/Data-Culpa/openclients) and configure your ```PYTHONPATH``` to include the ```dataculpa/``` directory.  (Coming soon: pip package.)
4. Create a .env file with the following keys:

```
# API key to access the storage.
DC_CONTROLLER_SECRET = secret-here   # Create a new API secret in the Data Culpa Validator UI
SNOWFLAKE_PASSWORD = secret-here
```

5. Run ```sfdatalake.py --init example.yaml``` to generate a template yaml to fill in connection coordinates. Note that we always keep secrets in the .env and not the yaml, so that the yaml file will be safe to check into source control or otherwise distribute in your organization, etc.


6. Once you have your yaml file edited, run ```sfdatalake.py --test example.yaml``` to test the connections to the database and the Data Culpa Validator controller.

7. (You can also run  ```sfdatalake.py --discover example.yaml``` to see what tables are discoverable for walking with the connector. Snowflake permissions may impact visibility here.)



## Invocation

The ```sfdatalake.py``` script is intended to be invoked from cron or other orchestration systems. You can run it as frequently as you wish; you can spread out instances to isolate collections or different databases with different yaml configuration files. We recommend pointing the script at a replica of your database to reduce impact on the life system.

## Future Improvements

There are many improvements we are considering for this module. You can get in touch by writing to hello@dataculpa.com or opening issues in this repository.

## SaaS deployment

Our hosted SaaS includes Snowflake and other connectors and a GUI for configuration. If you'd like to try it out, drop a line to hello@dataculpa.com.