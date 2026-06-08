"""
Current Neon database schema for the Alemdar Flow Flask service.

Generated from the Neon project `alemdar-flow` on 2026-06-08.
Project: icy-hill-95104916
Branch: production (br-orange-star-ab7sd6pz)
Database: alemdar-flow-flask-db
Schema: public
"""

from __future__ import annotations

from typing import Any, Dict, List


NEON_PROJECT_ID = "icy-hill-95104916"
NEON_BRANCH_ID = "br-orange-star-ab7sd6pz"
NEON_BRANCH_NAME = "production"
NEON_DATABASE_NAME = "alemdar-flow-flask-db"
NEON_SCHEMA_NAME = "public"


TABLE_SCHEMAS: Dict[str, Dict[str, Any]] = {
    "app_users": {
        "columns": [
            {
                "name": "id",
                "type": "bigint",
                "nullable": False,
                "default": "nextval('app_users_id_seq'::regclass)",
            },
            {
                "name": "customer_id",
                "type": "bigint",
                "nullable": True,
                "default": None,
            },
            {
                "name": "username",
                "type": "text",
                "nullable": False,
                "default": None,
            },
            {
                "name": "username_lower",
                "type": "text",
                "nullable": False,
                "default": None,
            },
            {
                "name": "password_hash",
                "type": "text",
                "nullable": False,
                "default": None,
            },
            {
                "name": "primary_inverter_serial",
                "type": "text",
                "nullable": False,
                "default": None,
            },
            {
                "name": "created_at",
                "type": "timestamp with time zone",
                "nullable": False,
                "default": "now()",
            },
            {
                "name": "updated_at",
                "type": "timestamp with time zone",
                "nullable": False,
                "default": "now()",
            },
        ],
        "indexes": [
            {
                "name": "app_users_pkey",
                "definition": (
                    "CREATE UNIQUE INDEX app_users_pkey ON public.app_users "
                    "USING btree (id)"
                ),
            },
            {
                "name": "app_users_username_lower_unique",
                "definition": (
                    "CREATE UNIQUE INDEX app_users_username_lower_unique "
                    "ON public.app_users USING btree (username_lower)"
                ),
            },
            {
                "name": "app_users_customer_id_idx",
                "definition": (
                    "CREATE INDEX app_users_customer_id_idx ON public.app_users "
                    "USING btree (customer_id)"
                ),
            },
            {
                "name": "app_users_primary_inverter_idx",
                "definition": (
                    "CREATE INDEX app_users_primary_inverter_idx ON public.app_users "
                    "USING btree (primary_inverter_serial)"
                ),
            },
        ],
        "constraints": [
            {
                "name": "app_users_pkey",
                "type": "PRIMARY KEY",
                "definition": "PRIMARY KEY (id)",
            },
            {
                "name": "app_users_username_lower_unique",
                "type": "UNIQUE",
                "definition": "UNIQUE (username_lower)",
            },
            {
                "name": "app_users_customer_id_fkey",
                "type": "FOREIGN KEY",
                "definition": (
                    "FOREIGN KEY (customer_id) REFERENCES customers(id) "
                    "ON DELETE SET NULL"
                ),
            },
        ],
    },
    "customer_inverters": {
        "columns": [
            {
                "name": "id",
                "type": "bigint",
                "nullable": False,
                "default": "nextval('customer_inverters_id_seq'::regclass)",
            },
            {
                "name": "customer_id",
                "type": "bigint",
                "nullable": False,
                "default": None,
            },
            {
                "name": "serial_number",
                "type": "text",
                "nullable": False,
                "default": None,
            },
            {
                "name": "created_at",
                "type": "timestamp with time zone",
                "nullable": False,
                "default": "now()",
            },
        ],
        "indexes": [
            {
                "name": "customer_inverters_pkey",
                "definition": (
                    "CREATE UNIQUE INDEX customer_inverters_pkey "
                    "ON public.customer_inverters USING btree (id)"
                ),
            },
            {
                "name": "customer_inverters_customer_serial_unique",
                "definition": (
                    "CREATE UNIQUE INDEX customer_inverters_customer_serial_unique "
                    "ON public.customer_inverters USING btree "
                    "(customer_id, serial_number)"
                ),
            },
            {
                "name": "customer_inverters_serial_unique",
                "definition": (
                    "CREATE UNIQUE INDEX customer_inverters_serial_unique "
                    "ON public.customer_inverters USING btree (serial_number)"
                ),
            },
            {
                "name": "customer_inverters_customer_id_idx",
                "definition": (
                    "CREATE INDEX customer_inverters_customer_id_idx "
                    "ON public.customer_inverters USING btree (customer_id)"
                ),
            },
        ],
        "constraints": [
            {
                "name": "customer_inverters_pkey",
                "type": "PRIMARY KEY",
                "definition": "PRIMARY KEY (id)",
            },
            {
                "name": "customer_inverters_customer_serial_unique",
                "type": "UNIQUE",
                "definition": "UNIQUE (customer_id, serial_number)",
            },
            {
                "name": "customer_inverters_serial_unique",
                "type": "UNIQUE",
                "definition": "UNIQUE (serial_number)",
            },
            {
                "name": "customer_inverters_customer_id_fkey",
                "type": "FOREIGN KEY",
                "definition": (
                    "FOREIGN KEY (customer_id) REFERENCES customers(id) "
                    "ON DELETE CASCADE"
                ),
            },
            {
                "name": "customer_inverters_serial_number_fkey",
                "type": "FOREIGN KEY",
                "definition": (
                    "FOREIGN KEY (serial_number) REFERENCES inverters(serial_number) "
                    "ON DELETE CASCADE"
                ),
            },
        ],
    },
    "customers": {
        "columns": [
            {
                "name": "id",
                "type": "bigint",
                "nullable": False,
                "default": "nextval('customers_id_seq'::regclass)",
            },
            {
                "name": "full_name",
                "type": "text",
                "nullable": False,
                "default": None,
            },
            {
                "name": "phone",
                "type": "text",
                "nullable": False,
                "default": None,
            },
            {
                "name": "email",
                "type": "text",
                "nullable": False,
                "default": None,
            },
            {
                "name": "location",
                "type": "text",
                "nullable": True,
                "default": None,
            },
            {
                "name": "nickname",
                "type": "text",
                "nullable": True,
                "default": None,
            },
            {
                "name": "date_installed",
                "type": "date",
                "nullable": True,
                "default": None,
            },
            {
                "name": "date_last_maintenance",
                "type": "date",
                "nullable": True,
                "default": None,
            },
            {
                "name": "address",
                "type": "text",
                "nullable": True,
                "default": None,
            },
            {
                "name": "notes",
                "type": "text",
                "nullable": True,
                "default": None,
            },
            {
                "name": "status",
                "type": "text",
                "nullable": False,
                "default": "'active'::text",
            },
            {
                "name": "created_at",
                "type": "timestamp with time zone",
                "nullable": False,
                "default": "now()",
            },
            {
                "name": "updated_at",
                "type": "timestamp with time zone",
                "nullable": False,
                "default": "now()",
            },
        ],
        "indexes": [
            {
                "name": "customers_pkey",
                "definition": (
                    "CREATE UNIQUE INDEX customers_pkey ON public.customers "
                    "USING btree (id)"
                ),
            },
            {
                "name": "customers_email_lower_idx",
                "definition": (
                    "CREATE UNIQUE INDEX customers_email_lower_idx "
                    "ON public.customers USING btree (lower(email))"
                ),
            },
            {
                "name": "customers_status_idx",
                "definition": (
                    "CREATE INDEX customers_status_idx ON public.customers "
                    "USING btree (status)"
                ),
            },
        ],
        "constraints": [
            {
                "name": "customers_pkey",
                "type": "PRIMARY KEY",
                "definition": "PRIMARY KEY (id)",
            },
        ],
    },
    "inverter_poll_audit": {
        "columns": [
            {
                "name": "id",
                "type": "bigint",
                "nullable": False,
                "default": "nextval('inverter_poll_audit_id_seq'::regclass)",
            },
            {
                "name": "serial_number",
                "type": "text",
                "nullable": False,
                "default": None,
            },
            {
                "name": "alias",
                "type": "text",
                "nullable": True,
                "default": None,
            },
            {
                "name": "polled_at",
                "type": "timestamp with time zone",
                "nullable": False,
                "default": "now()",
            },
            {
                "name": "status",
                "type": "text",
                "nullable": False,
                "default": None,
            },
            {
                "name": "attempts",
                "type": "integer",
                "nullable": False,
                "default": None,
            },
            {
                "name": "error_text",
                "type": "text",
                "nullable": True,
                "default": None,
            },
            {
                "name": "source",
                "type": "text",
                "nullable": False,
                "default": "'scheduler'::text",
            },
        ],
        "indexes": [
            {
                "name": "inverter_poll_audit_pkey",
                "definition": (
                    "CREATE UNIQUE INDEX inverter_poll_audit_pkey "
                    "ON public.inverter_poll_audit USING btree (id)"
                ),
            },
            {
                "name": "inverter_poll_audit_serial_polled_idx",
                "definition": (
                    "CREATE INDEX inverter_poll_audit_serial_polled_idx "
                    "ON public.inverter_poll_audit USING btree "
                    "(serial_number, polled_at DESC)"
                ),
            },
            {
                "name": "inverter_poll_audit_status_polled_idx",
                "definition": (
                    "CREATE INDEX inverter_poll_audit_status_polled_idx "
                    "ON public.inverter_poll_audit USING btree "
                    "(status, polled_at DESC)"
                ),
            },
        ],
        "constraints": [
            {
                "name": "inverter_poll_audit_pkey",
                "type": "PRIMARY KEY",
                "definition": "PRIMARY KEY (id)",
            },
        ],
    },
    "inverter_readings": {
        "columns": [
            {
                "name": "id",
                "type": "bigint",
                "nullable": False,
                "default": "nextval('inverter_readings_id_seq'::regclass)",
            },
            {
                "name": "serial_number",
                "type": "text",
                "nullable": False,
                "default": None,
            },
            {
                "name": "reading_at",
                "type": "timestamp with time zone",
                "nullable": False,
                "default": None,
            },
            {
                "name": "polled_at",
                "type": "timestamp with time zone",
                "nullable": False,
                "default": "now()",
            },
            {
                "name": "pv_power_w",
                "type": "numeric",
                "nullable": False,
                "default": None,
            },
            {
                "name": "load_power_w",
                "type": "numeric",
                "nullable": False,
                "default": None,
            },
            {
                "name": "grid_power_w",
                "type": "numeric",
                "nullable": False,
                "default": None,
            },
            {
                "name": "grid_voltage_v",
                "type": "numeric",
                "nullable": True,
                "default": None,
            },
            {
                "name": "grid_frequency_hz",
                "type": "numeric",
                "nullable": True,
                "default": None,
            },
            {
                "name": "raw_payload",
                "type": "jsonb",
                "nullable": False,
                "default": None,
            },
            {
                "name": "source",
                "type": "text",
                "nullable": False,
                "default": None,
            },
            {
                "name": "source_row_hash",
                "type": "text",
                "nullable": False,
                "default": None,
            },
            {
                "name": "created_at",
                "type": "timestamp with time zone",
                "nullable": False,
                "default": "now()",
            },
        ],
        "indexes": [
            {
                "name": "inverter_readings_pkey",
                "definition": (
                    "CREATE UNIQUE INDEX inverter_readings_pkey "
                    "ON public.inverter_readings USING btree (id)"
                ),
            },
            {
                "name": "inverter_readings_unique_source",
                "definition": (
                    "CREATE UNIQUE INDEX inverter_readings_unique_source "
                    "ON public.inverter_readings USING btree "
                    "(serial_number, reading_at, source_row_hash)"
                ),
            },
            {
                "name": "inverter_readings_serial_reading_idx",
                "definition": (
                    "CREATE INDEX inverter_readings_serial_reading_idx "
                    "ON public.inverter_readings USING btree "
                    "(serial_number, reading_at DESC)"
                ),
            },
            {
                "name": "inverter_readings_reading_idx",
                "definition": (
                    "CREATE INDEX inverter_readings_reading_idx "
                    "ON public.inverter_readings USING btree (reading_at DESC)"
                ),
            },
            {
                "name": "inverter_readings_serial_reading_hash_idx",
                "definition": (
                    "CREATE UNIQUE INDEX inverter_readings_serial_reading_hash_idx "
                    "ON public.inverter_readings USING btree "
                    "(serial_number, reading_at, source_row_hash)"
                ),
            },
            {
                "name": "inverter_readings_serial_reading_desc_idx",
                "definition": (
                    "CREATE INDEX inverter_readings_serial_reading_desc_idx "
                    "ON public.inverter_readings USING btree "
                    "(serial_number, reading_at DESC)"
                ),
            },
        ],
        "constraints": [
            {
                "name": "inverter_readings_pkey",
                "type": "PRIMARY KEY",
                "definition": "PRIMARY KEY (id)",
            },
            {
                "name": "inverter_readings_unique_source",
                "type": "UNIQUE",
                "definition": "UNIQUE (serial_number, reading_at, source_row_hash)",
            },
            {
                "name": "inverter_readings_serial_number_fkey",
                "type": "FOREIGN KEY",
                "definition": (
                    "FOREIGN KEY (serial_number) REFERENCES inverters(serial_number) "
                    "ON DELETE CASCADE"
                ),
            },
        ],
    },
    "inverters": {
        "columns": [
            {
                "name": "serial_number",
                "type": "text",
                "nullable": False,
                "default": None,
            },
            {
                "name": "alias",
                "type": "text",
                "nullable": True,
                "default": None,
            },
            {
                "name": "description",
                "type": "text",
                "nullable": True,
                "default": None,
            },
            {
                "name": "system_type",
                "type": "text",
                "nullable": True,
                "default": None,
            },
            {
                "name": "watchpower_username",
                "type": "text",
                "nullable": True,
                "default": None,
            },
            {
                "name": "wifi_pn",
                "type": "text",
                "nullable": True,
                "default": None,
            },
            {
                "name": "device_code",
                "type": "integer",
                "nullable": True,
                "default": None,
            },
            {
                "name": "device_address",
                "type": "integer",
                "nullable": True,
                "default": None,
            },
            {
                "name": "created_at",
                "type": "timestamp with time zone",
                "nullable": False,
                "default": "now()",
            },
            {
                "name": "updated_at",
                "type": "timestamp with time zone",
                "nullable": False,
                "default": "now()",
            },
        ],
        "indexes": [
            {
                "name": "inverters_pkey",
                "definition": (
                    "CREATE UNIQUE INDEX inverters_pkey ON public.inverters "
                    "USING btree (serial_number)"
                ),
            },
        ],
        "constraints": [
            {
                "name": "inverters_pkey",
                "type": "PRIMARY KEY",
                "definition": "PRIMARY KEY (serial_number)",
            },
        ],
    },
}


def get_table_schema(table_name: str) -> Dict[str, Any]:
    """Return schema metadata for a public table."""
    return TABLE_SCHEMAS[table_name]


def table_names() -> List[str]:
    """Return public table names in deterministic order."""
    return sorted(TABLE_SCHEMAS)
