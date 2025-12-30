import os

# Maritime compliance calculations - MRV, DCS, EU ETS, CII
compliance_q1 = """-- Query: EU MRV (Monitoring, Reporting, Verification) Compliance Calculations
-- Complexity: Highly Complex
-- Description: Calculate CO2 emissions, fuel consumption, and distance for EU MRV reporting
-- Compliance: EU Regulation 2015/757 (MRV), evidence trail with voyage-level aggregation

WITH voyage_telemetry_agg AS (
    SELECT 
        v.tenant_id,
        v.vessel_imo,
        v.voyage_id,
        v.departure_port,
        v.arrival_port,
        v.departure_time,
        v.estimated_arrival_time,
        -- Check if voyage is EU-relevant (simplified - either port in EU)
        CASE 
            WHEN v.departure_port IN ('NLRTM', 'DEHAM', 'BEANR', 'FRLEH', 'ESVLC') 
                OR v.arrival_port IN ('NLRTM', 'DEHAM', 'BEANR', 'FRLEH', 'ESVLC')
            THEN TRUE ELSE FALSE
        END AS is_eu_voyage,
        -- Aggregate telemetry data
        COUNT(DISTINCT t.timestamp) AS telemetry_points,
        SUM(t.fuel_consumption_mt) AS total_fuel_consumed_mt,
        AVG(t.speed_knots) AS avg_speed_knots,
        SUM(t.distance_traveled_nm) AS total_distance_nm,
        MIN(t.timestamp) AS actual_departure,
        MAX(t.timestamp) AS actual_arrival
    FROM maritime_iceberg.maritime.voyages v
    INNER JOIN maritime_iceberg.maritime.vessel_telemetry t
        ON v.voyage_id = t.voyage_id AND v.tenant_id = t.tenant_id
    WHERE v.tenant_id = '${tenant_id}'
        AND v.departure_time >= DATE '2024-01-01'
        AND v.departure_time < DATE '2025-01-01'
    GROUP BY 
        v.tenant_id, v.vessel_imo, v.voyage_id, v.departure_port, v.arrival_port,
        v.departure_time, v.estimated_arrival_time
),
mrv_calculations AS (
    SELECT 
        vta.*,
        -- CO2 emissions calculations (using IPCC conversion factors)
        -- HFO: 3.114 tonnes CO2 per tonne fuel
        -- MDO/MGO: 3.206 tonnes CO2 per tonne fuel
        -- Assuming HFO for main engine (simplified)
        vta.total_fuel_consumed_mt * 3.114 AS co2_emissions_tonnes,
        -- Calculate transport work (tonne-nautical miles)
        -- Would need cargo weight from voyage metadata
        COALESCE(
            CAST(json_extract_scalar(v.voyage_metadata, '$.cargo.total_weight_tonnes') AS DOUBLE),
            0
        ) * vta.total_distance_nm AS transport_work_tnm,
        -- Voyage duration
        (CAST(vta.actual_arrival AS BIGINT) - CAST(vta.actual_departure AS BIGINT)) / 3600.0 AS voyage_duration_hours,
        -- Time at sea (simplified)
        vta.telemetry_points AS hours_at_sea
    FROM voyage_telemetry_agg vta
    INNER JOIN maritime_iceberg.maritime.voyages v
        ON vta.voyage_id = v.voyage_id
    WHERE vta.is_eu_voyage = TRUE
)
SELECT 
    tenant_id,
    vessel_imo,
    voyage_id,
    departure_port,
    arrival_port,
    actual_departure AS departure_timestamp,
    actual_arrival AS arrival_timestamp,
    voyage_duration_hours,
    total_distance_nm,
    total_fuel_consumed_mt,
    co2_emissions_tonnes,
    transport_work_tnm,
    avg_speed_knots,
    -- MRV reporting metrics
    (co2_emissions_tonnes / NULLIF(total_distance_nm, 0)) AS co2_per_nm,
    (co2_emissions_tonnes / NULLIF(transport_work_tnm, 0)) * 1000000 AS co2_per_million_tnm,
    (total_fuel_consumed_mt / NULLIF(total_distance_nm, 0)) AS fuel_efficiency_mt_per_nm,
    -- Annual aggregation helpers
    YEAR(actual_departure) AS reporting_year,
    QUARTER(actual_departure) AS reporting_quarter,
    -- Evidence trail
    telemetry_points AS data_points_count,
    CASE 
        WHEN telemetry_points >= (voyage_duration_hours * 0.9) THEN 'HIGH_QUALITY'
        WHEN telemetry_points >= (voyage_duration_hours * 0.7) THEN 'ACCEPTABLE'
        ELSE 'LOW_QUALITY'
    END AS data_quality_flag
FROM mrv_calculations
ORDER BY actual_departure DESC;
"""

compliance_q2 = """-- Query: IMO DCS (Data Collection System) and CII (Carbon Intensity Indicator) Calculations
-- Complexity: Highly Complex
-- Description: Calculate CII rating (A-E) and DCS metrics per vessel per year
-- Compliance: IMO MARPOL Annex VI, MEPC.328(76) - CII rating system with evidence

WITH annual_voyage_data AS (
    SELECT 
        v.tenant_id,
        v.vessel_imo,
        YEAR(v.departure_time) AS reporting_year,
        v.voyage_id,
        v.departure_time,
        v.estimated_arrival_time,
        SUM(t.fuel_consumption_mt) AS voyage_fuel_mt,
        SUM(t.distance_traveled_nm) AS voyage_distance_nm,
        COUNT(DISTINCT t.timestamp) AS voyage_data_points
    FROM maritime_iceberg.maritime.voyages v
    INNER JOIN maritime_iceberg.maritime.vessel_telemetry t
        ON v.voyage_id = t.voyage_id AND v.tenant_id = t.tenant_id
    WHERE v.tenant_id = '${tenant_id}'
        AND v.departure_time >= DATE '2024-01-01'
        AND v.departure_time < DATE '2025-01-01'
    GROUP BY v.tenant_id, v.vessel_imo, v.voyage_id, v.departure_time, v.estimated_arrival_time
),
vessel_annual_summary AS (
    SELECT 
        avd.tenant_id,
        avd.vessel_imo,
        avd.reporting_year,
        COUNT(DISTINCT avd.voyage_id) AS total_voyages,
        SUM(avd.voyage_fuel_mt) AS annual_fuel_consumption_mt,
        SUM(avd.voyage_distance_nm) AS annual_distance_nm,
        SUM(avd.voyage_data_points) AS total_data_points,
        -- Get vessel details from compliance table (or use fixed values for demo)
        COALESCE(
            CAST(json_extract_scalar(vc.vessel_metadata, '$.deadweight_tonnes') AS DOUBLE),
            50000
        ) AS deadweight_tonnes,
        COALESCE(
            json_extract_scalar(vc.vessel_metadata, '$.vessel_type'),
            'BULK_CARRIER'
        ) AS vessel_type
    FROM annual_voyage_data avd
    LEFT JOIN maritime_iceberg.maritime.vessel_compliance vc
        ON avd.vessel_imo = vc.vessel_imo AND avd.tenant_id = vc.tenant_id
    GROUP BY avd.tenant_id, avd.vessel_imo, avd.reporting_year, vc.vessel_metadata
),
cii_calculations AS (
    SELECT 
        vas.*,
        -- Calculate CO2 emissions (using HFO conversion factor)
        vas.annual_fuel_consumption_mt * 3.114 AS annual_co2_emissions_tonnes,
        -- CII = CO2 emissions / (Deadweight * Distance)
        (vas.annual_fuel_consumption_mt * 3.114) / 
        NULLIF(vas.deadweight_tonnes * vas.annual_distance_nm, 0) AS attained_cii,
        -- Required CII (simplified - actual formula depends on vessel type and year)
        -- Formula: Required CII = a * DWT^-c * (1 - Z/100)
        -- For bulk carriers: a=4745, c=0.622, Z varies by year
        CASE 
            WHEN vas.vessel_type = 'BULK_CARRIER' THEN 
                4745 * POW(vas.deadweight_tonnes, -0.622) * (1 - 0.05)  -- 5% reduction for 2024
            WHEN vas.vessel_type = 'CONTAINER' THEN 
                1984 * POW(vas.deadweight_tonnes, -0.489) * (1 - 0.05)
            WHEN vas.vessel_type = 'TANKER' THEN 
                5247 * POW(vas.deadweight_tonnes, -0.610) * (1 - 0.05)
            ELSE 
                3000 * POW(vas.deadweight_tonnes, -0.5) * (1 - 0.05)
        END AS required_cii
    FROM vessel_annual_summary vas
),
cii_ratings AS (
    SELECT 
        cc.*,
        -- Calculate CII rating boundaries (simplified)
        cc.required_cii * 0.87 AS rating_a_threshold,
        cc.required_cii * 0.94 AS rating_b_threshold,
        cc.required_cii * 1.06 AS rating_c_threshold,
        cc.required_cii * 1.18 AS rating_d_threshold,
        -- Assign rating
        CASE 
            WHEN cc.attained_cii <= cc.required_cii * 0.87 THEN 'A'
            WHEN cc.attained_cii <= cc.required_cii * 0.94 THEN 'B'
            WHEN cc.attained_cii <= cc.required_cii * 1.06 THEN 'C'
            WHEN cc.attained_cii <= cc.required_cii * 1.18 THEN 'D'
            ELSE 'E'
        END AS cii_rating,
        -- Calculate percentage vs required
        ((cc.attained_cii - cc.required_cii) / NULLIF(cc.required_cii, 0)) * 100 AS cii_percentage_vs_required
    FROM cii_calculations cc
)
SELECT 
    tenant_id,
    vessel_imo,
    reporting_year,
    vessel_type,
    deadweight_tonnes,
    total_voyages,
    annual_distance_nm,
    annual_fuel_consumption_mt,
    annual_co2_emissions_tonnes,
    attained_cii,
    required_cii,
    cii_rating,
    cii_percentage_vs_required,
    rating_a_threshold,
    rating_b_threshold,
    rating_c_threshold,
    rating_d_threshold,
    -- DCS reporting metrics
    (annual_co2_emissions_tonnes / NULLIF(annual_distance_nm, 0)) AS co2_per_nm_dcs,
    (annual_fuel_consumption_mt / NULLIF(annual_distance_nm, 0)) AS fuel_per_nm_dcs,
    -- Evidence quality
    total_data_points,
    CASE 
        WHEN total_data_points >= (total_voyages * 24 * 30) THEN 'VERIFIED'
        WHEN total_data_points >= (total_voyages * 24 * 20) THEN 'ACCEPTABLE'
        ELSE 'REQUIRES_REVIEW'
    END AS dcs_data_quality,
    -- Compliance status
    CASE 
        WHEN cii_rating IN ('D', 'E') THEN 'ACTION_REQUIRED'
        WHEN cii_rating = 'C' THEN 'MONITOR'
        ELSE 'COMPLIANT'
    END AS compliance_status
FROM cii_ratings
ORDER BY cii_rating, vessel_imo;
"""

compliance_q3 = """-- Query: EU ETS (Emissions Trading System) Allowance Calculations
-- Complexity: Highly Complex
-- Description: Calculate EU ETS obligations with phase-in and evidence trail
-- Compliance: EU Directive 2003/87/EC as amended, phase-in 2024-2026

WITH eu_port_list AS (
    -- EU and EEA ports (simplified list)
    SELECT port_code FROM (
        VALUES ('NLRTM'), ('DEHAM'), ('BEANR'), ('FRLEH'), ('ESVLC'), 
               ('ITGOA'), ('GRATH'), ('PLGDN'), ('SEMMA'), ('FIKTK')
    ) AS ports(port_code)
),
voyage_emissions AS (
    SELECT 
        v.tenant_id,
        v.vessel_imo,
        v.voyage_id,
        v.departure_port,
        v.arrival_port,
        v.departure_time,
        -- Classify voyage segments for EU ETS
        CASE 
            WHEN v.departure_port IN (SELECT port_code FROM eu_port_list) 
                AND v.arrival_port IN (SELECT port_code FROM eu_port_list)
            THEN 'INTRA_EU'
            WHEN v.departure_port IN (SELECT port_code FROM eu_port_list) 
                AND v.arrival_port NOT IN (SELECT port_code FROM eu_port_list)
            THEN 'EU_DEPARTURE'
            WHEN v.departure_port NOT IN (SELECT port_code FROM eu_port_list) 
                AND v.arrival_port IN (SELECT port_code FROM eu_port_list)
            THEN 'EU_ARRIVAL'
            ELSE 'NON_EU'
        END AS voyage_classification,
        SUM(t.fuel_consumption_mt) AS total_fuel_mt,
        SUM(t.fuel_consumption_mt) * 3.114 AS total_co2_tonnes
    FROM maritime_iceberg.maritime.voyages v
    INNER JOIN maritime_iceberg.maritime.vessel_telemetry t
        ON v.voyage_id = t.voyage_id AND v.tenant_id = t.tenant_id
    WHERE v.tenant_id = '${tenant_id}'
        AND v.departure_time >= DATE '2024-01-01'
        AND v.departure_time < DATE '2025-01-01'
    GROUP BY v.tenant_id, v.vessel_imo, v.voyage_id, v.departure_port, v.arrival_port, v.departure_time
),
ets_liability AS (
    SELECT 
        ve.*,
        YEAR(ve.departure_time) AS reporting_year,
        -- EU ETS phase-in percentages
        CASE 
            WHEN YEAR(ve.departure_time) = 2024 THEN 0.40
            WHEN YEAR(ve.departure_time) = 2025 THEN 0.70
            WHEN YEAR(ve.departure_time) >= 2026 THEN 1.00
            ELSE 0.00
        END AS ets_phase_in_factor,
        -- Calculate reportable emissions per EU ETS rules
        CASE 
            WHEN ve.voyage_classification = 'INTRA_EU' 
                THEN ve.total_co2_tonnes  -- 100% of emissions
            WHEN ve.voyage_classification IN ('EU_DEPARTURE', 'EU_ARRIVAL') 
                THEN ve.total_co2_tonnes * 0.50  -- 50% of emissions
            ELSE 0
        END AS reportable_co2_tonnes,
        -- At-berth emissions (simplified - 20% of fuel at EU ports)
        CASE 
            WHEN ve.voyage_classification IN ('INTRA_EU', 'EU_ARRIVAL') 
                THEN ve.total_co2_tonnes * 0.20
            ELSE 0
        END AS berth_co2_tonnes
    FROM voyage_emissions ve
    WHERE ve.voyage_classification != 'NON_EU'
),
annual_ets_summary AS (
    SELECT 
        tenant_id,
        vessel_imo,
        reporting_year,
        ets_phase_in_factor,
        COUNT(DISTINCT voyage_id) AS eu_relevant_voyages,
        SUM(reportable_co2_tonnes) AS total_reportable_co2,
        SUM(berth_co2_tonnes) AS total_berth_co2,
        SUM(CASE WHEN voyage_classification = 'INTRA_EU' THEN reportable_co2_tonnes ELSE 0 END) AS intra_eu_co2,
        SUM(CASE WHEN voyage_classification = 'EU_DEPARTURE' THEN reportable_co2_tonnes ELSE 0 END) AS departure_co2,
        SUM(CASE WHEN voyage_classification = 'EU_ARRIVAL' THEN reportable_co2_tonnes ELSE 0 END) AS arrival_co2
    FROM ets_liability
    GROUP BY tenant_id, vessel_imo, reporting_year, ets_phase_in_factor
)
SELECT 
    tenant_id,
    vessel_imo,
    reporting_year,
    eu_relevant_voyages,
    total_reportable_co2,
    total_berth_co2,
    intra_eu_co2,
    departure_co2,
    arrival_co2,
    ets_phase_in_factor,
    -- Calculate allowances required
    (total_reportable_co2 + total_berth_co2) * ets_phase_in_factor AS ets_allowances_required,
    -- Estimated cost (assuming €80 per allowance)
    (total_reportable_co2 + total_berth_co2) * ets_phase_in_factor * 80 AS estimated_ets_cost_eur,
    -- Breakdown by voyage type
    intra_eu_co2 * ets_phase_in_factor AS intra_eu_allowances,
    (departure_co2 + arrival_co2) * ets_phase_in_factor AS international_allowances,
    total_berth_co2 * ets_phase_in_factor AS berth_allowances,
    -- Compliance metadata
    CONCAT(
        'Year: ', CAST(reporting_year AS VARCHAR), 
        ' | Phase-in: ', CAST(CAST(ets_phase_in_factor * 100 AS INT) AS VARCHAR), '%',
        ' | Voyages: ', CAST(eu_relevant_voyages AS VARCHAR)
    ) AS compliance_summary
FROM annual_ets_summary
ORDER BY vessel_imo, reporting_year;
"""

# Save compliance calculation queries
os.makedirs(f'{presto_queries_dir}/compliance-calculations', exist_ok=True)

comp_q1_path = f'{presto_queries_dir}/compliance-calculations/01_eu_mrv_reporting.sql'
with open(comp_q1_path, 'w') as f:
    f.write(compliance_q1)

comp_q2_path = f'{presto_queries_dir}/compliance-calculations/02_imo_dcs_cii_ratings.sql'
with open(comp_q2_path, 'w') as f:
    f.write(compliance_q2)

comp_q3_path = f'{presto_queries_dir}/compliance-calculations/03_eu_ets_allowances.sql'
with open(comp_q3_path, 'w') as f:
    f.write(compliance_q3)

print("Maritime Compliance Calculation Queries Created:")
print(f"1. EU MRV Reporting: {comp_q1_path}")
print(f"2. IMO DCS & CII Ratings: {comp_q2_path}")
print(f"3. EU ETS Allowance Calculations: {comp_q3_path}")
print("\nCompliance Capabilities:")
print("- EU MRV (Monitoring, Reporting, Verification)")
print("- IMO DCS (Data Collection System)")
print("- IMO CII (Carbon Intensity Indicator) with A-E ratings")
print("- EU ETS (Emissions Trading System) with phase-in")
print("- Evidence trails and data quality flags")
print("- Multi-year aggregations and rollups")
