-- Current Neon database schema for the Alemdar Flow Flask service.
-- Derived from `alemdar-flow-flask/services/neon_schema.py`.
--
-- Notes:
-- - PRIMARY KEY and UNIQUE constraints create their backing indexes implicitly,
--   so those same-name indexes from the Python snapshot are not repeated here.
-- - Secondary indexes from the snapshot are included below, including redundant
--   duplicates that appear to exist in the current database.

CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE public.customers (
    id BIGSERIAL NOT NULL,
    full_name TEXT NOT NULL,
    phone TEXT NOT NULL,
    email TEXT NOT NULL,
    location TEXT,
    nickname TEXT,
    date_installed DATE,
    date_last_maintenance DATE,
    address TEXT,
    notes TEXT,
    status TEXT NOT NULL DEFAULT 'active'::text,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT customers_pkey PRIMARY KEY (id)
);

CREATE TABLE public.inverters (
    serial_number TEXT NOT NULL,
    alias TEXT,
    description TEXT,
    system_type TEXT,
    watchpower_username TEXT,
    wifi_pn TEXT,
    device_code INTEGER,
    device_address INTEGER,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT inverters_pkey PRIMARY KEY (serial_number)
);

CREATE TABLE public.app_users (
    id BIGSERIAL NOT NULL,
    customer_id BIGINT,
    username TEXT NOT NULL,
    username_lower TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    primary_inverter_serial TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT app_users_pkey PRIMARY KEY (id),
    CONSTRAINT app_users_username_lower_unique UNIQUE (username_lower),
    CONSTRAINT app_users_customer_id_fkey
        FOREIGN KEY (customer_id)
        REFERENCES public.customers(id)
        ON DELETE SET NULL
);

CREATE TABLE public.customer_inverters (
    id BIGSERIAL NOT NULL,
    customer_id BIGINT NOT NULL,
    serial_number TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT customer_inverters_pkey PRIMARY KEY (id),
    CONSTRAINT customer_inverters_customer_serial_unique
        UNIQUE (customer_id, serial_number),
    CONSTRAINT customer_inverters_serial_unique UNIQUE (serial_number),
    CONSTRAINT customer_inverters_customer_id_fkey
        FOREIGN KEY (customer_id)
        REFERENCES public.customers(id)
        ON DELETE CASCADE,
    CONSTRAINT customer_inverters_serial_number_fkey
        FOREIGN KEY (serial_number)
        REFERENCES public.inverters(serial_number)
        ON DELETE CASCADE
);

CREATE TABLE public.inverter_poll_audit (
    id BIGSERIAL NOT NULL,
    serial_number TEXT NOT NULL,
    alias TEXT,
    polled_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    error_text TEXT,
    source TEXT NOT NULL DEFAULT 'scheduler'::text,
    CONSTRAINT inverter_poll_audit_pkey PRIMARY KEY (id)
);

CREATE TABLE public.inverter_readings (
    id BIGSERIAL NOT NULL,
    serial_number TEXT NOT NULL,
    reading_at TIMESTAMP WITH TIME ZONE NOT NULL,
    polled_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    pv_power_w NUMERIC(12,2) NOT NULL,
    load_power_w NUMERIC(12,2) NOT NULL,
    grid_power_w NUMERIC(12,2) NOT NULL,
    grid_voltage_v NUMERIC(8,2),
    grid_frequency_hz NUMERIC(8,2),
    raw_payload JSONB NOT NULL,
    source TEXT NOT NULL,
    source_row_hash TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT inverter_readings_pkey PRIMARY KEY (id),
    CONSTRAINT inverter_readings_unique_source
        UNIQUE (serial_number, reading_at, source_row_hash),
    CONSTRAINT inverter_readings_serial_number_fkey
        FOREIGN KEY (serial_number)
        REFERENCES public.inverters(serial_number)
        ON DELETE CASCADE
);

CREATE INDEX app_users_customer_id_idx
    ON public.app_users USING btree (customer_id);

CREATE INDEX app_users_primary_inverter_idx
    ON public.app_users USING btree (primary_inverter_serial);

CREATE INDEX customer_inverters_customer_id_idx
    ON public.customer_inverters USING btree (customer_id);

CREATE UNIQUE INDEX customers_email_lower_idx
    ON public.customers USING btree (lower(email));

CREATE INDEX customers_status_idx
    ON public.customers USING btree (status);

CREATE INDEX inverter_poll_audit_serial_polled_idx
    ON public.inverter_poll_audit USING btree (serial_number, polled_at DESC);

CREATE INDEX inverter_poll_audit_status_polled_idx
    ON public.inverter_poll_audit USING btree (status, polled_at DESC);

CREATE UNIQUE INDEX inverter_readings_serial_reading_hash_idx
    ON public.inverter_readings USING btree (
        serial_number,
        reading_at,
        source_row_hash
    );

CREATE INDEX inverter_readings_reading_idx
    ON public.inverter_readings USING btree (reading_at DESC);

CREATE INDEX inverter_readings_serial_reading_desc_idx
    ON public.inverter_readings USING btree (serial_number, reading_at DESC);

CREATE INDEX inverter_readings_serial_reading_idx
    ON public.inverter_readings USING btree (serial_number, reading_at DESC);
