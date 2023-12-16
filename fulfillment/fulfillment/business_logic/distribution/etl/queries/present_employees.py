from fulfillment.helpers.environment.env import get_region_id


def get_present_employees_query() -> str:
    return f"""
        DROP TABLE IF EXISTS fcs;
        CREATE TEMP TABLE fcs AS (SELECT warehouse_id
                                FROM fulfillment_fcs
                                WHERE region_id = {get_region_id()}
                                    AND is_hub = 1
                                    AND business_type IN ('DCS')
                                    AND is_active = 1
                                GROUP BY 1
                                UNION ALL
                                SELECT 'RNO4' AS warehouse_id);

        WITH clock_in AS (
            SELECT
                CASE WHEN warehouse_id = 'RNO4' THEN 'LAS6' ELSE warehouse_id END AS warehouse_id,
                employee_id,
                event_time_utc,
                event_type,
                RANK() OVER(PARTITION BY employee_id ORDER BY event_time_utc DESC) AS rank
            FROM time_clock_event
            WHERE event_time_utc >= CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - INTERVAL '1 day'
                AND warehouse_id IN (SELECT warehouse_id FROM fcs)
        )

        SELECT
            warehouse_id,
            employee_id,
            ROUND(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)) AS ingressTimestamp,
            ROUND(EXTRACT(EPOCH FROM event_time_utc))::varchar(255) AS event_datetime
        FROM clock_in
        WHERE rank = 1 AND event_type = 'In'
            AND EXTRACT(HOUR FROM CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - event_time_utc) < 2;
        """