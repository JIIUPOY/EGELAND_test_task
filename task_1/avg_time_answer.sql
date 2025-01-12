WITH message_groups AS (
    SELECT
        cm.entity_id,
        cm.message_id,
        cm.created_by,
        cm.created_at,
        cm.type,
        CASE
            WHEN LAG(cm.type) OVER (PARTITION BY cm.entity_id ORDER BY cm.created_at) = cm.type THEN 0
            ELSE 1
        END AS is_first_message_in_block
    FROM test.chat_messages cm
),
working_hours_messages AS (
    SELECT
        mg.entity_id,
        mg.message_id,
        mg.created_by,
        mg.created_at,
        mg.type,
        mg.is_first_message_in_block,
        CASE
            WHEN EXTRACT(HOUR FROM to_timestamp(mg.created_at)) < 9 OR
                 (EXTRACT(HOUR FROM to_timestamp(mg.created_at)) = 9 AND EXTRACT(MINUTE FROM to_timestamp(mg.created_at)) < 30)
            THEN
                (date_trunc('day', to_timestamp(mg.created_at)) + INTERVAL '9 hour 30 minute')
            ELSE
                to_timestamp(mg.created_at)
        END AS adjusted_created_at
    FROM message_groups mg
    WHERE mg.is_first_message_in_block = 1
),
response_times AS (
    SELECT
        m1.entity_id,
        m1.created_by AS user_id,
        m2.created_at AS response_time,
        m2.created_by AS manager_id,
        EXTRACT(EPOCH FROM
            case
            when extract(day from to_timestamp(m1.created_at)) <> extract(day from to_timestamp(m2.created_at))
              then (
                CASE
                      WHEN EXTRACT(HOUR FROM to_timestamp(m2.created_at)) < 9 OR
                           (EXTRACT(HOUR FROM to_timestamp(m2.created_at)) = 9 AND EXTRACT(MINUTE FROM to_timestamp(m2.created_at)) < 30)
                      THEN
                          (date_trunc('day', to_timestamp(m2.created_at)) + INTERVAL '9 hour 30 minute')
                      ELSE
                          to_timestamp(m2.created_at)
                  end
                  - (date_trunc('day', to_timestamp(m2.created_at)) + INTERVAL '9 hour 30 minute')
                  + (date_trunc('day', to_timestamp(m1.created_at)) + INTERVAL '1 day' - to_timestamp(m1.created_at))
                  )
                else
                  CASE
                      WHEN EXTRACT(HOUR FROM to_timestamp(m2.created_at)) < 9 OR
                           (EXTRACT(HOUR FROM to_timestamp(m2.created_at)) = 9 AND EXTRACT(MINUTE FROM to_timestamp(m2.created_at)) < 30)
                      THEN
                          (date_trunc('day', to_timestamp(m2.created_at)) + INTERVAL '9 hour 30 minute')
                      ELSE
                          to_timestamp(m2.created_at)
                  end
                  - m1.adjusted_created_at
      end) / 60 AS response_minutes
    FROM working_hours_messages m1
    JOIN working_hours_messages m2
        ON m1.entity_id = m2.entity_id
        AND m1.created_by = 0
        and m1."type" = 'incoming_chat_message'
        AND m2.created_by <> 0
        and m2."type" = 'outgoing_chat_message'
        AND m2.created_at > m1.created_at
)
SELECT
    rt.manager_id,
    m.name_mop as manager_name,
    AVG(response_minutes) AS avg_response_time_minutes
FROM response_times rt
JOIN test.managers m on m.mop_id = rt.manager_id
GROUP BY manager_id, m.name_mop
ORDER BY avg_response_time_minutes
;