import pandas as pd
import numpy as np
from typing import Tuple, Optional
from dataclasses import dataclass
from datetime import datetime

# Port Database with berth availability
port_database = pd.DataFrame({
    'port_name': [
        'Singapore', 'Rotterdam', 'Shanghai', 'Los Angeles', 'Hamburg',
        'Hong Kong', 'Antwerp', 'Dubai', 'New York', 'Tokyo',
        'Busan', 'Kaohsiung', 'Piraeus', 'Mumbai', 'Santos',
        'Vancouver', 'Sydney', 'Cape Town', 'Suez', 'Panama City'
    ],
    'latitude': [
        1.2644, 51.9244, 31.2304, 33.7406, 53.5511,
        22.3193, 51.2194, 25.2048, 40.7128, 35.6762,
        35.1796, 22.6273, 37.9413, 18.9220, -23.9608,
        49.2827, -33.8688, -33.9249, 30.0444, 8.9824
    ],
    'longitude': [
        103.8520, 4.4777, 121.4737, -118.2703, 9.9937,
        114.1694, 4.4025, 55.2708, -74.0060, 139.6503,
        129.0756, 120.2654, 23.6474, 72.8347, -46.3333,
        -123.1207, 151.2093, 18.4241, 32.5538, -79.5199
    ],
    'berths_total': [200, 180, 220, 150, 120, 160, 140, 100, 130, 110, 105, 90, 85, 95, 70, 80, 75, 60, 50, 45],
    'berths_available': [45, 32, 55, 28, 22, 35, 30, 18, 25, 20, 22, 18, 15, 20, 12, 15, 14, 10, 8, 9],
    'avg_wait_hours': [12, 18, 15, 24, 20, 14, 16, 10, 22, 16, 14, 12, 18, 20, 15, 12, 11, 8, 10, 14],
    'weather_zone': [
        'SEA_TROPICAL', 'EUR_TEMPERATE', 'CHN_SUBTROPICAL', 'USA_PACIFIC', 'EUR_TEMPERATE',
        'CHN_SUBTROPICAL', 'EUR_TEMPERATE', 'ME_DESERT', 'USA_ATLANTIC', 'JPN_TEMPERATE',
        'KOR_TEMPERATE', 'TWN_SUBTROPICAL', 'MED_MEDITERRANEAN', 'IND_MONSOON', 'SAM_TROPICAL',
        'CAN_TEMPERATE', 'AUS_TEMPERATE', 'SAF_TEMPERATE', 'ME_DESERT', 'CAM_TROPICAL'
    ]
})

# Weather zone definitions for vessel routing
weather_zones = {
    'SEA_TROPICAL': {'storm_risk': 0.15, 'cyclone_season': 'Jun-Nov'},
    'EUR_TEMPERATE': {'storm_risk': 0.25, 'cyclone_season': 'Oct-Mar'},
    'CHN_SUBTROPICAL': {'storm_risk': 0.20, 'cyclone_season': 'Jul-Oct'},
    'USA_PACIFIC': {'storm_risk': 0.10, 'cyclone_season': 'Jun-Nov'},
    'USA_ATLANTIC': {'storm_risk': 0.18, 'cyclone_season': 'Jun-Nov'},
    'MED_MEDITERRANEAN': {'storm_risk': 0.12, 'cyclone_season': 'Sep-Dec'},
    'IND_MONSOON': {'storm_risk': 0.30, 'cyclone_season': 'Apr-Nov'},
    'ME_DESERT': {'storm_risk': 0.08, 'cyclone_season': None},
    'JPN_TEMPERATE': {'storm_risk': 0.22, 'cyclone_season': 'Aug-Oct'},
    'KOR_TEMPERATE': {'storm_risk': 0.20, 'cyclone_season': 'Jul-Sep'},
    'TWN_SUBTROPICAL': {'storm_risk': 0.25, 'cyclone_season': 'Jul-Oct'},
    'SAM_TROPICAL': {'storm_risk': 0.18, 'cyclone_season': 'Jan-Mar'},
    'CAN_TEMPERATE': {'storm_risk': 0.20, 'cyclone_season': 'Oct-Mar'},
    'AUS_TEMPERATE': {'storm_risk': 0.15, 'cyclone_season': 'Nov-Apr'},
    'SAF_TEMPERATE': {'storm_risk': 0.20, 'cyclone_season': 'May-Oct'},
    'CAM_TROPICAL': {'storm_risk': 0.22, 'cyclone_season': 'May-Nov'}
}

@dataclass
class GeoEnrichment:
    """Result of geospatial enrichment"""
    nearest_port_name: str
    distance_to_port_nm: float
    weather_zone: str
    berths_available: int
    avg_wait_hours: float
    storm_risk: float

def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate great-circle distance in nautical miles between two points
    Uses Haversine formula
    """
    R = 3440.065  # Earth radius in nautical miles
    
    lat1_rad = np.radians(lat1)
    lat2_rad = np.radians(lat2)
    delta_lat = np.radians(lat2 - lat1)
    delta_lon = np.radians(lon2 - lon1)
    
    a = np.sin(delta_lat/2)**2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(delta_lon/2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))
    
    return R * c

def find_nearest_port(vessel_lat: float, vessel_lon: float, ports_df: pd.DataFrame) -> Tuple[str, float, pd.Series]:
    """
    Find nearest port and calculate distance using great-circle distance
    Returns: (port_name, distance_nm, port_details)
    """
    distances = ports_df.apply(
        lambda row: haversine_distance(vessel_lat, vessel_lon, row['latitude'], row['longitude']),
        axis=1
    )
    
    nearest_idx = distances.idxmin()
    nearest_port = ports_df.loc[nearest_idx]
    
    return nearest_port['port_name'], distances[nearest_idx], nearest_port

def enrich_vessel_position(vessel_lat: float, vessel_lon: float, ports_df: pd.DataFrame) -> GeoEnrichment:
    """
    Enrich a vessel position with geospatial data
    """
    port_name, distance_nm, port_details = find_nearest_port(vessel_lat, vessel_lon, ports_df)
    
    weather_zone = port_details['weather_zone']
    weather_info = weather_zones.get(weather_zone, {'storm_risk': 0.0})
    
    return GeoEnrichment(
        nearest_port_name=port_name,
        distance_to_port_nm=round(distance_nm, 2),
        weather_zone=weather_zone,
        berths_available=int(port_details['berths_available']),
        avg_wait_hours=float(port_details['avg_wait_hours']),
        storm_risk=weather_info['storm_risk']
    )

# Test with sample vessel positions
sample_vessels = pd.DataFrame({
    'vessel_id': ['IMO-9876543', 'IMO-8765432', 'IMO-7654321', 'IMO-6543210'],
    'timestamp': [datetime.now()] * 4,
    'latitude': [1.3, 51.5, 31.5, 22.4],
    'longitude': [103.9, 4.3, 121.2, 114.0]
})

# Enrich sample vessels
enriched_results = []
for _, vessel in sample_vessels.iterrows():
    enrichment = enrich_vessel_position(
        vessel['latitude'], 
        vessel['longitude'], 
        port_database
    )
    enriched_results.append({
        'vessel_id': vessel['vessel_id'],
        'latitude': vessel['latitude'],
        'longitude': vessel['longitude'],
        'nearest_port_name': enrichment.nearest_port_name,
        'distance_to_port_nm': enrichment.distance_to_port_nm,
        'weather_zone': enrichment.weather_zone,
        'berths_available': enrichment.berths_available,
        'avg_wait_hours': enrichment.avg_wait_hours,
        'storm_risk': enrichment.storm_risk
    })

enriched_vessels = pd.DataFrame(enriched_results)

print("=" * 80)
print("GEOSPATIAL ENRICHMENT PIPELINE - PORT DATABASE & DISTANCE CALCULATION")
print("=" * 80)
print(f"\nPort Database: {len(port_database)} major global ports")
print(f"Weather Zones: {len(weather_zones)} defined zones")
print(f"\nSample Enrichment Results:")
print(enriched_vessels.to_string(index=False))
print(f"\n✓ Great-circle distance calculation (Haversine formula)")
print(f"✓ Nearest port reverse geocoding")
print(f"✓ Weather zone lookup with storm risk")
print(f"✓ Berth availability integration")
