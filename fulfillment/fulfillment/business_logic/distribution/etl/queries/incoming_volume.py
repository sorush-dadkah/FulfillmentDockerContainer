from fulfillment.helpers.environment.env import (
    get_region_id,
    get_region_name,
)


def get_incoming_volume_query(stage: str) -> str:
    return f"""
        WITH incoming_volume_temp_zone AS (
            SELECT
                warehouse_id,
                expected_arrival_datetime,
                CASE WHEN temp_zone IS NULL THEN 'AMBIENT' ELSE temp_zone END AS temp_zone,
                SUM(expected_qty) AS units
            FROM fulfillment
            WHERE ib_flow = 'DR'
                AND region_id = '{get_region_id()}'
                AND DATE(expected_arrival_date) BETWEEN CURRENT_DATE - 1 AND CURRENT_DATE + 6
                AND snapshot_time_ts = (SELECT MAX(snapshot_time_ts) FROM orecast_hourly)
                AND hub_or_spoke = 'HUB'
                AND business_type = 'DCS'
            GROUP BY warehouse_id, expected_arrival_datetime, temp_zone
        ),
        upc_and_cpp_4_wk_avg AS (
            SELECT
                warehouse_id,
                NULLIF(SUM(unit_quantity), 0) / NULLIF(SUM(case_count), 0) AS upc_4_wk_avg,
                NULLIF(SUM(case_count), 0) / SUM(CASE WHEN pkg_type = 'Pallet' THEN total_volume ELSE NULL END) cpp_4_wk_avg
            FROM production_rates
            WHERE aggregate_row_ind = '0'
                AND COALESCE(task_nm, 'Not') NOT LIKE '%Total'
                AND region = '{get_region_name().upper()}'
                AND event_date >= CURRENT_DATE-28
            GROUP BY warehouse_id
        ),
        agg_tbl AS (
            SELECT
                a.*,
                NULLIF(a.units / b.upc_4_wk_avg, 0) AS cases,
                NULLIF(a.units / b.cpp_4_wk_avg, 0) AS pallets
            FROM temp_zone a
            JOIN upc_and_cpp_4_wk_avg b
                ON a.warehouse_id = b.warehouse_id
        ),
        final_tbl AS (
            SELECT
                warehouse_id,
                expected_arrival_datetime,
                temp_zone,
                SUM(units) AS units,
                SUM(cases) AS cases,
                SUM(pallets) AS pallets
            FROM agg_tbl
            GROUP BY 1, 2, 3
        )
        SELECT
            '{stage.upper()}' || warehouse_id || '#AGL#' || EXTRACT(YEAR FROM expected_arrival_datetime) || '#' || EXTRACT(MONTH FROM expected_arrival_datetime) AS partition,
            'INPUT#volume#incoming#' || LOWER(temp_zone) || '#' || ROUND(EXTRACT(EPOCH FROM expected_arrival_datetime)) AS entity,
            ROUND(EXTRACT(EPOCH FROM expected_arrival_datetime)) AS source_timestamp,
            ROUND(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)) AS ingress_timestamp,
            units,
            cases::float4::float8::numeric,
            pallets::float4::float8::numeric
        FROM final_tbl;
    """
