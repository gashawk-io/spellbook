{{ config(
    schema='gas',
    alias='usage_by_address',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    partition_by=['blockchain'],
    unique_key=['address', 'blockchain', 'currency_symbol'],
    post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum", "fantom", "polygon", "base", "celo", "zora", "zksync", "scroll", "linea", "zkevm", "sonic"]\',
                        "sector",
                        "gas",
                        \'["gashawk", "zohjag"]\') }}'
) }}

WITH 
{% if is_incremental() %}
-- Check if max_block_time column exists in current table
column_exists AS (
    SELECT COUNT(*) as has_max_block_time
    FROM information_schema.columns 
    WHERE table_schema = '{{ this.schema }}'
      AND table_name = '{{ this.identifier }}'
      AND column_name = 'max_block_time'
),

-- If max_block_time doesn't exist, we'll do a full refresh
{% raw %}
full_refresh_needed AS (
    SELECT (SELECT has_max_block_time FROM column_exists) = 0 as needs_full_refresh
),
{% endraw %}

-- Get current metrics from the materialized table if it has the right structure
current_metrics AS (
    SELECT 
        address, 
        blockchain, 
        currency_symbol, 
        number_of_txs, 
        gas_spent_usd_total, 
        gas_spent_usd_1_year,
        gas_spent_native_total, 
        gas_spent_native_1_year,
        max_block_time
    FROM {{ this }}
    {% raw %}
    WHERE (SELECT needs_full_refresh FROM full_refresh_needed) = false
    {% endraw %}
),

-- Only process recent data (30 days) for short-term metrics
recent_gas_costs AS (
    SELECT 
        blockchain,
        tx_from as address,
        currency_symbol,
        block_time,
        tx_fee as gas_cost_native,
        tx_fee_usd as gas_cost_usd
    FROM {{ source('gas', 'fees') }}
    WHERE currency_symbol is not null
    AND block_time >= now() - INTERVAL '30' DAY
),

-- Process new transactions since last update
new_gas_costs AS (
    SELECT 
        blockchain,
        tx_from as address,
        currency_symbol,
        block_time,
        tx_fee as gas_cost_native,
        tx_fee_usd as gas_cost_usd
    FROM {{ source('gas', 'fees') }}
    WHERE currency_symbol is not null
    {% raw %}
    AND CASE WHEN (SELECT needs_full_refresh FROM full_refresh_needed) = true
             THEN true
             ELSE block_time > (SELECT MAX(max_block_time) FROM current_metrics)
         END
    {% endraw %}
),

-- Identify transactions falling out of the 1-year window
window_to_forget AS (
    SELECT
        tx_from as address,
        blockchain,
        currency_symbol,
        SUM(tx_fee_usd) as gas_spent_usd,
        SUM(tx_fee) as gas_spent_native
    FROM {{ source('gas', 'fees') }} gf
    JOIN current_metrics cm
        ON cm.address = gf.tx_from 
        AND cm.blockchain = gf.blockchain 
        AND cm.currency_symbol = gf.currency_symbol
    WHERE block_time > cm.max_block_time - INTERVAL '1' YEAR 
        AND block_time < now() - INTERVAL '1' YEAR
    {% raw %}
    AND (SELECT needs_full_refresh FROM full_refresh_needed) = false
    {% endraw %}
    GROUP BY 1, 2, 3
),

-- Get all historical data if we need a full refresh
full_gas_costs AS (
    SELECT 
        blockchain,
        tx_from as address,
        currency_symbol,
        block_time,
        tx_fee as gas_cost_native,
        tx_fee_usd as gas_cost_usd
    FROM {{ source('gas', 'fees') }}
    WHERE currency_symbol is not null
    {% raw %}
    AND (SELECT needs_full_refresh FROM full_refresh_needed) = true
    {% endraw %}
),

-- Combine all gas costs based on whether we need a full refresh
combined_gas_costs AS (
    SELECT * FROM recent_gas_costs
    UNION ALL
    SELECT * FROM new_gas_costs
    UNION ALL
    SELECT * FROM full_gas_costs
),

updated_metrics AS (
    SELECT
        COALESCE(cg.address, cm.address) as address,
        COALESCE(cg.blockchain, cm.blockchain) as blockchain,
        COALESCE(cg.currency_symbol, cm.currency_symbol) as currency_symbol,
        
        -- Optimize number_of_txs calculation
        COALESCE(cm.number_of_txs, 0) + COUNT(CASE WHEN cg.block_time > COALESCE(cm.max_block_time, '1970-01-01') THEN 1 END) as number_of_txs,
        
        -- Optimize total metrics (add new transactions to existing totals)
        COALESCE(cm.gas_spent_usd_total, 0) + SUM(CASE WHEN cg.block_time > COALESCE(cm.max_block_time, '1970-01-01') THEN cg.gas_cost_usd ELSE 0 END) as gas_spent_usd_total,
        COALESCE(cm.gas_spent_native_total, 0) + SUM(CASE WHEN cg.block_time > COALESCE(cm.max_block_time, '1970-01-01') THEN cg.gas_cost_native ELSE 0 END) as gas_spent_native_total,
        
        -- Calculate short-term metrics from recent data only
        SUM(CASE WHEN cg.block_time >= now() - INTERVAL '1' DAY THEN cg.gas_cost_usd ELSE 0 END) as gas_spent_usd_24_hours,
        SUM(CASE WHEN cg.block_time >= now() - INTERVAL '7' DAY THEN cg.gas_cost_usd ELSE 0 END) as gas_spent_usd_7_days,
        SUM(CASE WHEN cg.block_time >= now() - INTERVAL '30' DAY THEN cg.gas_cost_usd ELSE 0 END) as gas_spent_usd_30_days,
        SUM(CASE WHEN cg.block_time >= now() - INTERVAL '1' DAY THEN cg.gas_cost_native ELSE 0 END) as gas_spent_native_24_hours,
        SUM(CASE WHEN cg.block_time >= now() - INTERVAL '7' DAY THEN cg.gas_cost_native ELSE 0 END) as gas_spent_native_7_days,
        SUM(CASE WHEN cg.block_time >= now() - INTERVAL '30' DAY THEN cg.gas_cost_native ELSE 0 END) as gas_spent_native_30_days,
        
        -- Optimize 1-year metrics (add new transactions, subtract transactions falling out of window)
        COALESCE(cm.gas_spent_usd_1_year, 0) + 
            SUM(CASE WHEN cg.block_time > COALESCE(cm.max_block_time, '1970-01-01') AND cg.block_time >= now() - INTERVAL '1' YEAR THEN cg.gas_cost_usd ELSE 0 END) - 
            COALESCE(wf.gas_spent_usd, 0) as gas_spent_usd_1_year,
        COALESCE(cm.gas_spent_native_1_year, 0) + 
            SUM(CASE WHEN cg.block_time > COALESCE(cm.max_block_time, '1970-01-01') AND cg.block_time >= now() - INTERVAL '1' YEAR THEN cg.gas_cost_native ELSE 0 END) - 
            COALESCE(wf.gas_spent_native, 0) as gas_spent_native_1_year,
            
        -- Update max_block_time
        GREATEST(COALESCE(cm.max_block_time, '1970-01-01'), MAX(cg.block_time)) as max_block_time
    FROM combined_gas_costs cg
    FULL OUTER JOIN current_metrics cm
        ON cm.address = cg.address 
        AND cm.blockchain = cg.blockchain 
        AND cm.currency_symbol = cg.currency_symbol
    LEFT JOIN window_to_forget wf
        ON wf.address = COALESCE(cg.address, cm.address)
        AND wf.blockchain = COALESCE(cg.blockchain, cm.blockchain)
        AND wf.currency_symbol = COALESCE(cg.currency_symbol, cm.currency_symbol)
    GROUP BY 1, 2, 3, cm.number_of_txs, cm.gas_spent_usd_total, cm.gas_spent_native_total, 
             cm.gas_spent_usd_1_year, cm.gas_spent_native_1_year, cm.max_block_time, 
             wf.gas_spent_usd, wf.gas_spent_native
)
{% else %}
-- Initial run - get all data and calculate metrics from scratch
gas_costs AS (
    SELECT 
        blockchain,
        tx_from as address,
        currency_symbol,
        block_time,
        tx_fee as gas_cost_native,
        tx_fee_usd as gas_cost_usd
    FROM {{ source('gas', 'fees') }}
    WHERE currency_symbol is not null
),

updated_metrics AS (
    SELECT
        address,
        blockchain,
        currency_symbol,
        COUNT(*) as number_of_txs,
        SUM(gas_cost_usd) as gas_spent_usd_total,
        SUM(CASE WHEN block_time >= now() - INTERVAL '1' DAY THEN gas_cost_usd END) as gas_spent_usd_24_hours,
        SUM(CASE WHEN block_time >= now() - INTERVAL '7' DAY THEN gas_cost_usd END) as gas_spent_usd_7_days,
        SUM(CASE WHEN block_time >= now() - INTERVAL '30' DAY THEN gas_cost_usd END) as gas_spent_usd_30_days,
        SUM(CASE WHEN block_time >= now() - INTERVAL '1' YEAR THEN gas_cost_usd END) as gas_spent_usd_1_year,
        SUM(gas_cost_native) as gas_spent_native_total,
        SUM(CASE WHEN block_time >= now() - INTERVAL '1' DAY THEN gas_cost_native END) as gas_spent_native_24_hours,
        SUM(CASE WHEN block_time >= now() - INTERVAL '7' DAY THEN gas_cost_native END) as gas_spent_native_7_days,
        SUM(CASE WHEN block_time >= now() - INTERVAL '30' DAY THEN gas_cost_native END) as gas_spent_native_30_days,
        SUM(CASE WHEN block_time >= now() - INTERVAL '1' YEAR THEN gas_cost_native END) as gas_spent_native_1_year,
        MAX(block_time) as max_block_time
    FROM gas_costs
    GROUP BY 1, 2, 3
)
{% endif %}

SELECT 
    address,
    blockchain,
    currency_symbol,
    number_of_txs,
    gas_spent_usd_total,
    gas_spent_usd_24_hours,
    gas_spent_usd_7_days,
    gas_spent_usd_30_days,
    gas_spent_usd_1_year,
    gas_spent_native_total,
    gas_spent_native_24_hours,
    gas_spent_native_7_days,
    gas_spent_native_30_days,
    gas_spent_native_1_year,
    (max_block_time) as last_block_time_of_incremental_update
FROM updated_metrics
ORDER BY gas_spent_usd_total DESC