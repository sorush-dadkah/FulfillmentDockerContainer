from fulfillment.helpers.environment.env import get_stage

base_props = {
    "schema_suffix": "_dev" if get_stage() in ["alpha", "gamma"] else "",
    "postgresql": {
        "host_mapping": {
            "read": "fulfillment-db-dev-pg-node-1.us-east-1.rds.amazonaws.com",
            "write": "fulfillment-db-dev-pg-node-2.us-east-1.rds.amazonaws.com",
        },
        "user": "fulfillment",
        "database": "fulfillment_pgsql",
        "port": 5439,
        "sts": {
            "boto3_client": "rds",
            "role_arn": "arn:aws:iam::123:role/fulfillment",
            "role_session_name": "fulfillment_pgsql_session",
            "region_name": "us-east-1",
        },
    },
    "mysql": {
        "host_mapping": {
            "read": "fulfillment-db-dev-node-1.us-east-1.rds.amazonaws.com",
            "write": "fulfillment-db-dev-node-2.us-east-1.rds.amazonaws.com",
        },
        "user": "fulfillment",
        "database": "fulfillment_mysql",
        "port": 5439,
        "sts": {
            "boto3_client": "rds",
            "role_arn": "arn:aws:iam::123:role/fulfillment",
            "role_session_name": "fulfillment_mysql_session",
            "region_name": "us-east-1",
        },
    },
    "redshift": {
        "host_mapping": {"read": "fulfillment-2.cueufaslo.us-east-1.redshift.amazonaws.com"},
        "user": "IAMR:Fulfillment",
        "database": "fulfillment_rs_2",
        "port": 5439,
        "sts": {
            "boto3_client": "redshift",
            "cluster_identifier": "fulfillment-2",
            "role_arn": "arn:aws:iam::123:role/fulfillment",
            "role_session_name": "fulfillment_redshift_session",
            "duration_seconds": 3600,
            "region_name": "us-east-1",
        },
    },
    "dynamodb": {
        "sts": {
            "boto3_client": "dynamodb",
            "role_arn": "arn:aws:iam::123:role/fulfillment",
            "role_session_name": "fulfillment_dynamodb_session",
            "region_name": "us-east-1",
        }
    },
}

alpha_props = base_props
gamma_props = base_props
# alpha_props['new_db'] = {'role_arn': 'example', 'region_name': 'test'}  # how to add a new database configuration

DB_PROPS: dict[str, dict[str, any]] = {
    "prod": base_props,
    "gamma": gamma_props,
    "alpha": alpha_props,
}
