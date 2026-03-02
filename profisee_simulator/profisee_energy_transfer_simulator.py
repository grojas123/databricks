"""
===============================================================================
Profisee MDM Connector Simulator for Databricks
===============================================================================
Simulates the output of the Profisee MDM native connector landing golden-record
master data into Databricks Delta tables — modeled for Energy Transfer LP.

Profisee Connect writes governed, deduplicated master data into the Lakehouse
following the medallion architecture. This simulator produces DataFrames that
mirror the schema and metadata columns the real connector would generate.

Usage (Databricks notebook):
    %run ./profisee_energy_transfer_simulator
    # or:  from profisee_energy_transfer_simulator import ProfiseeMDMSimulator
    sim = ProfiseeMDMSimulator(seed=42)
    sim.generate_all()
    sim.write_delta_tables(catalog="mdm", schema="energy_transfer")

Usage (local / unit-test):
    python profisee_energy_transfer_simulator.py   # writes Parquet to ./output/
===============================================================================
"""

from __future__ import annotations

import hashlib
import json
import random
import string
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import numpy as np


# ---------------------------------------------------------------------------
# Constants — Energy Transfer domain knowledge
# ---------------------------------------------------------------------------

# Real Energy Transfer business segments (from 10-K filings)
BUSINESS_SEGMENTS = [
    "Intrastate Transportation and Storage",
    "Interstate Transportation and Storage",
    "Midstream",
    "NGL and Refined Products Transportation and Services",
    "Crude Oil Transportation and Services",
    "Investment in Sunoco LP",
    "Investment in USAC",
    "All Other",
]

# Real pipeline systems (public knowledge)
PIPELINE_SYSTEMS = [
    "Panhandle Eastern", "Trunkline", "Rover", "Tiger",
    "Transwestern", "Florida Gas Transmission", "Oasis",
    "Lone Star", "Texas Eastern Transmission (TETCO)",
    "ETC Fayetteville Express", "Regency Intrastate",
    "Houston Pipeline", "Heritage", "Midcontinent Express",
    "RIGS", "Mariner East", "Bakken Pipeline (Dakota Access)",
    "Bayou Bridge", "White Cliffs", "Maurepas",
    "Red Bluff Express", "Permian Express",
    "Desert Southwest Pipeline (planned)",
]

# Facility types in the midstream energy business
FACILITY_TYPES = [
    "Gas Processing Plant", "NGL Fractionation Facility",
    "Crude Oil Terminal", "NGL Storage Cavern",
    "Compressor Station", "LNG Regasification Terminal",
    "Refined Products Terminal", "Truck Loading Rack",
    "Pipeline Interconnect", "Measurement Station",
    "Gas Treating Facility", "Dehydration Facility",
    "Tank Farm", "Blending Facility",
]

# Major production basins where ET operates
PRODUCTION_BASINS = [
    "Permian Basin", "Eagle Ford Shale", "Marcellus Shale",
    "Utica Shale", "Haynesville Shale", "Barnett Shale",
    "Bakken Formation", "Anadarko Basin", "DJ Basin",
    "San Juan Basin", "Fayetteville Shale", "Appalachian Basin",
    "Midcontinent", "Austin Chalk",
]

STATES = [
    "TX", "LA", "PA", "OH", "WV", "OK", "NM", "CO",
    "ND", "MT", "KS", "IL", "IN", "MS", "AL", "FL",
    "GA", "NY", "NJ", "MI", "AR", "TN",
]

PRODUCT_TYPES = [
    "Natural Gas", "Ethane", "Propane", "Normal Butane",
    "Isobutane", "Natural Gasoline", "Condensate",
    "Crude Oil - WTI", "Crude Oil - WTS", "Crude Oil - LLS",
    "Reformulated Blendstock (RBOB)", "Ultra Low Sulfur Diesel (ULSD)",
    "Jet Fuel", "Residual Fuel Oil", "LNG",
    "Mixed NGLs (Y-Grade)", "Purity Ethylene",
]

CUSTOMER_TYPES = [
    "Electric Utility", "Local Distribution Company (LDC)",
    "Industrial End-User", "Petrochemical Plant",
    "Refinery", "Marketing Company", "Power Plant (IPP)",
    "Municipal Utility", "Export Terminal Operator",
    "Data Center Operator", "LNG Export Facility",
]

VENDOR_CATEGORIES = [
    "Pipeline Construction", "Integrity Management",
    "SCADA & Instrumentation", "Compression Services",
    "Environmental & Compliance", "Cathodic Protection",
    "Valve & Actuator Supplier", "Pipe & Fittings Supplier",
    "Land & Right-of-Way", "Trucking & Logistics",
    "IT & Cybersecurity", "Legal & Regulatory",
    "Engineering & Design", "Inspection (NDE/NDT)",
]

# Profisee MDM-specific status values
PROFISEE_RECORD_STATUSES = ["Active", "Inactive", "Pending Review", "Soft-Deleted"]
PROFISEE_MATCH_STATUSES = ["Golden Record", "Matched", "Potential Match", "Unique"]
PROFISEE_VALIDATION_STATUSES = ["Passed", "Warning", "Failed"]
PROFISEE_SOURCES = [
    "SAP ERP", "Salesforce CRM", "Maximo EAM",
    "Manual Entry", "SCADA System", "GIS Platform",
    "Legacy Mainframe", "Workday HR", "ServiceNow",
    "Excel Upload", "API Import", "Lakeflow Connect",
]


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _profisee_code(prefix: str, idx: int) -> str:
    """Generate a Profisee-style entity code, e.g. PIPE-00042."""
    return f"{prefix}-{idx:05d}"


def _match_code() -> str:
    """Generate a Profisee match group identifier."""
    return f"MG-{uuid.uuid4().hex[:8].upper()}"


def _quality_score(rng: np.random.Generator) -> float:
    """Simulate a DQ score (0-100) skewed toward high quality (golden records)."""
    return round(float(np.clip(rng.beta(8, 2) * 100, 0, 100)), 2)


def _profisee_metadata(
    rng: np.random.Generator,
    code: str,
    entity_name: str,
    is_golden: bool = True,
) -> dict[str, Any]:
    """
    Return the metadata columns that the Profisee connector appends to every
    record when it writes to Databricks Delta tables.
    """
    base_dt = datetime(2024, 1, 1) + timedelta(days=int(rng.integers(0, 500)))
    updated_dt = base_dt + timedelta(days=int(rng.integers(0, 90)))

    status = "Active" if rng.random() > 0.08 else rng.choice(PROFISEE_RECORD_STATUSES[1:])
    match_status = "Golden Record" if is_golden else rng.choice(PROFISEE_MATCH_STATUSES[1:])
    source = rng.choice(PROFISEE_SOURCES)
    validation = "Passed" if rng.random() > 0.12 else rng.choice(PROFISEE_VALIDATION_STATUSES[1:])

    return {
        "_profisee_code": code,
        "_profisee_entity": entity_name,
        "_profisee_record_status": status,
        "_profisee_match_status": match_status,
        "_profisee_match_code": _match_code() if match_status != "Unique" else None,
        "_profisee_data_quality_score": _quality_score(rng),
        "_profisee_validation_status": validation,
        "_profisee_source_system": source,
        "_profisee_created_dtm": base_dt.isoformat(),
        "_profisee_updated_dtm": updated_dt.isoformat(),
        "_profisee_created_by": rng.choice(["mdm_admin", "data_steward_01", "api_service", "etl_pipeline"]),
        "_profisee_updated_by": rng.choice(["mdm_admin", "data_steward_01", "api_service", "etl_pipeline", "steward_review"]),
        "_profisee_version": int(rng.integers(1, 12)),
        "_profisee_is_merged": bool(rng.random() > 0.7),
        "_profisee_export_batch_id": f"BATCH-{uuid.uuid4().hex[:12].upper()}",
    }


# ---------------------------------------------------------------------------
# Simulator class
# ---------------------------------------------------------------------------

class ProfiseeMDMSimulator:
    """
    Generates realistic Profisee MDM golden-record data for an Energy Transfer
    -style midstream energy company and writes it as Delta / Parquet tables.

    Master data domains generated
    ─────────────────────────────
    1. dim_pipeline_assets     — pipeline systems, segments, specs
    2. dim_facilities          — processing plants, terminals, storage
    3. dim_customers           — utilities, industrials, marketers
    4. dim_vendors             — suppliers and service providers
    5. dim_products            — commodities and product specs
    6. dim_locations           — geographic sites and GPS coords
    7. dim_employees           — contacts, operators, stewards
    8. dim_regulatory          — permits, compliance items
    9. dim_contracts           — transportation & storage agreements
    10. fact_throughput_daily   — simulated daily volume measurements
    """

    def __init__(self, seed: int = 42):
        self.rng = np.random.default_rng(seed)
        self.random = random.Random(seed)
        self._tables: dict[str, pd.DataFrame] = {}

    # ── 1. Pipeline Assets ────────────────────────────────────────────────

    def _generate_pipeline_assets(self, n: int = 200) -> pd.DataFrame:
        rows = []
        for i in range(1, n + 1):
            code = _profisee_code("PIPE", i)
            system = self.rng.choice(PIPELINE_SYSTEMS)
            segment_id = f"{system[:3].upper()}-SEG-{i:04d}"
            diameter = self.rng.choice([6, 8, 10, 12, 16, 20, 24, 30, 36, 42])
            length_mi = round(float(self.rng.uniform(2, 450)), 1)
            maop = int(self.rng.choice([720, 1000, 1200, 1440, 1480, 2160]))
            material = self.rng.choice(["Carbon Steel", "High-Strength Steel (X70)", "High-Strength Steel (X80)"])
            commodity = self.rng.choice(["Natural Gas", "Crude Oil", "NGL", "Refined Products", "Mixed NGLs"])
            basin = self.rng.choice(PRODUCTION_BASINS)
            state = self.rng.choice(STATES)
            install_year = int(self.rng.integers(1960, 2025))
            capacity_mmbtu_d = int(self.rng.integers(50_000, 5_000_000)) if "Gas" in commodity else None
            capacity_bpd = int(self.rng.integers(10_000, 800_000)) if "Gas" not in commodity else None

            row = {
                "pipeline_system": system,
                "segment_id": segment_id,
                "business_segment": self.rng.choice(BUSINESS_SEGMENTS[:5]),
                "diameter_inches": diameter,
                "length_miles": length_mi,
                "max_allowable_operating_pressure_psig": maop,
                "pipe_material": material,
                "commodity_transported": commodity,
                "production_basin": basin,
                "state": state,
                "installation_year": install_year,
                "last_inline_inspection_date": (
                    datetime(2020, 1, 1) + timedelta(days=int(self.rng.integers(0, 1800)))
                ).strftime("%Y-%m-%d"),
                "capacity_mmbtu_per_day": capacity_mmbtu_d,
                "capacity_barrels_per_day": capacity_bpd,
                "is_interstate": bool(self.rng.random() > 0.5),
                "ferc_regulated": bool(self.rng.random() > 0.45),
                **_profisee_metadata(self.rng, code, "PipelineAsset"),
            }
            rows.append(row)
        return pd.DataFrame(rows)

    # ── 2. Facilities ─────────────────────────────────────────────────────

    def _generate_facilities(self, n: int = 120) -> pd.DataFrame:
        rows = []
        for i in range(1, n + 1):
            code = _profisee_code("FAC", i)
            ftype = self.rng.choice(FACILITY_TYPES)
            state = self.rng.choice(STATES)
            lat = round(float(self.rng.uniform(26.0, 48.5)), 6)
            lon = round(float(self.rng.uniform(-104.0, -80.0)), 6)
            cap_unit = "MMcf/d" if "Gas" in ftype else ("bpd" if "Crude" in ftype or "NGL" in ftype else "bbl")
            capacity = int(self.rng.integers(5_000, 2_000_000))

            row = {
                "facility_name": f"ET {ftype} - {state}-{i:03d}",
                "facility_type": ftype,
                "business_segment": self.rng.choice(BUSINESS_SEGMENTS[:5]),
                "state": state,
                "county": f"{self.random.choice(['Harris', 'Jefferson', 'Caddo', 'Webb', 'Washington', 'Mountrail', 'Reeves', 'Midland'])} County",
                "latitude": lat,
                "longitude": lon,
                "nameplate_capacity": capacity,
                "capacity_uom": cap_unit,
                "year_commissioned": int(self.rng.integers(1970, 2025)),
                "is_operational": bool(self.rng.random() > 0.06),
                "connected_pipeline_system": self.rng.choice(PIPELINE_SYSTEMS),
                "h2s_treating_capable": bool(self.rng.random() > 0.6),
                "environmental_permit_id": f"EP-{state}-{self.rng.integers(100000, 999999)}",
                **_profisee_metadata(self.rng, code, "Facility"),
            }
            rows.append(row)
        return pd.DataFrame(rows)

    # ── 3. Customers ──────────────────────────────────────────────────────

    def _generate_customers(self, n: int = 300) -> pd.DataFrame:
        rows = []
        company_suffixes = ["Energy", "Power", "Gas", "Utilities", "Corp", "LLC", "Inc", "Partners", "Resources"]
        city_names = ["Houston", "Dallas", "Tulsa", "Pittsburgh", "Midland", "Shreveport", "Philadelphia",
                      "Chicago", "New Orleans", "Oklahoma City", "Denver", "Phoenix", "Charleston"]

        for i in range(1, n + 1):
            code = _profisee_code("CUST", i)
            ctype = self.rng.choice(CUSTOMER_TYPES)
            state = self.rng.choice(STATES)
            city = self.rng.choice(city_names)

            row = {
                "customer_name": f"{self.random.choice(['Apex', 'Summit', 'Valley', 'Ridge', 'Prairie', 'Coastal', 'Northern', 'Southern', 'Central', 'Western', 'Eastern', 'Continental', 'National', 'Metro', 'Regional', 'Heartland'])} {self.random.choice(company_suffixes)}",
                "customer_type": ctype,
                "duns_number": f"{self.rng.integers(100000000, 999999999)}",
                "tax_id": f"{self.rng.integers(10, 99)}-{self.rng.integers(1000000, 9999999)}",
                "city": city,
                "state": state,
                "credit_rating": self.rng.choice(["AAA", "AA+", "AA", "AA-", "A+", "A", "A-", "BBB+", "BBB", "BB+"]),
                "contract_type": self.rng.choice(["Firm Transportation", "Interruptible", "Storage", "Processing", "Fractionation"]),
                "primary_commodity": self.rng.choice(PRODUCT_TYPES[:7]),
                "annual_volume_contracted_mmbtu": int(self.rng.integers(100_000, 50_000_000)),
                "payment_terms_days": int(self.rng.choice([15, 20, 25, 30, 45, 60])),
                "is_key_account": bool(self.rng.random() > 0.8),
                "account_manager": f"AM-{self.rng.integers(100, 500)}",
                **_profisee_metadata(self.rng, code, "Customer"),
            }
            rows.append(row)
        return pd.DataFrame(rows)

    # ── 4. Vendors ────────────────────────────────────────────────────────

    def _generate_vendors(self, n: int = 180) -> pd.DataFrame:
        rows = []
        for i in range(1, n + 1):
            code = _profisee_code("VEND", i)
            category = self.rng.choice(VENDOR_CATEGORIES)

            row = {
                "vendor_name": f"{self.random.choice(['Atlas', 'Pinnacle', 'Frontier', 'Meridian', 'Patriot', 'Titan', 'Quantum', 'Vanguard', 'Keystone', 'Sentinel', 'Ironclad', 'Nexus'])} {self.random.choice(['Services', 'Solutions', 'Industries', 'Group', 'Corp', 'Technologies', 'Engineering'])}",
                "vendor_category": category,
                "duns_number": f"{self.rng.integers(100000000, 999999999)}",
                "state": self.rng.choice(STATES),
                "is_approved": bool(self.rng.random() > 0.1),
                "safety_rating": round(float(self.rng.uniform(0.5, 2.5)), 2),  # TRIR
                "insurance_expiry_date": (datetime(2025, 1, 1) + timedelta(days=int(self.rng.integers(0, 730)))).strftime("%Y-%m-%d"),
                "minority_owned": bool(self.rng.random() > 0.85),
                "small_business": bool(self.rng.random() > 0.7),
                "payment_terms_days": int(self.rng.choice([30, 45, 60, 90])),
                "annual_spend_usd": round(float(self.rng.uniform(50_000, 25_000_000)), 2),
                "contract_end_date": (datetime(2025, 6, 1) + timedelta(days=int(self.rng.integers(0, 1095)))).strftime("%Y-%m-%d"),
                **_profisee_metadata(self.rng, code, "Vendor"),
            }
            rows.append(row)
        return pd.DataFrame(rows)

    # ── 5. Products ───────────────────────────────────────────────────────

    def _generate_products(self, n: int = 50) -> pd.DataFrame:
        rows = []
        for i, product in enumerate(PRODUCT_TYPES * 3, 1):  # repeat to reach n
            if i > n:
                break
            code = _profisee_code("PROD", i)
            is_gas = "Gas" in product or "LNG" in product
            row = {
                "product_name": product,
                "product_category": self.rng.choice(["Natural Gas", "NGL", "Crude Oil", "Refined Products", "LNG"]),
                "measurement_uom": "MMBtu" if is_gas else "Barrels",
                "api_gravity": round(float(self.rng.uniform(28, 45)), 1) if "Crude" in product else None,
                "btu_content": round(float(self.rng.uniform(900, 1200)), 1) if is_gas else None,
                "sulfur_pct": round(float(self.rng.uniform(0.01, 3.5)), 3) if "Crude" in product else None,
                "is_hedgeable": bool(self.rng.random() > 0.3),
                "nymex_ticker": self.rng.choice(["NG", "CL", "HO", "RB", None]),
                "regulatory_class": self.rng.choice(["FERC-regulated", "State-regulated", "Unregulated"]),
                **_profisee_metadata(self.rng, code, "Product"),
            }
            rows.append(row)
        return pd.DataFrame(rows)

    # ── 6. Locations ──────────────────────────────────────────────────────

    def _generate_locations(self, n: int = 250) -> pd.DataFrame:
        rows = []
        location_types = [
            "Pipeline Right-of-Way", "Meter Station", "Valve Site",
            "Compressor Station", "Office", "Terminal", "Tank Farm",
            "Processing Plant", "Pump Station", "Pig Launcher/Receiver",
        ]
        for i in range(1, n + 1):
            code = _profisee_code("LOC", i)
            state = self.rng.choice(STATES)
            lat = round(float(self.rng.uniform(26.0, 48.5)), 6)
            lon = round(float(self.rng.uniform(-104.0, -80.0)), 6)

            row = {
                "location_name": f"ET-{state}-{self.rng.choice(location_types).split()[0]}-{i:04d}",
                "location_type": self.rng.choice(location_types),
                "state": state,
                "latitude": lat,
                "longitude": lon,
                "production_basin": self.rng.choice(PRODUCTION_BASINS + [None, None]),
                "connected_pipeline": self.rng.choice(PIPELINE_SYSTEMS),
                "milepost": round(float(self.rng.uniform(0, 800)), 2),
                "elevation_ft": int(self.rng.integers(0, 8000)),
                "has_power": bool(self.rng.random() > 0.15),
                "has_communication": bool(self.rng.random() > 0.1),
                "access_road": bool(self.rng.random() > 0.2),
                "last_survey_date": (datetime(2022, 1, 1) + timedelta(days=int(self.rng.integers(0, 1200)))).strftime("%Y-%m-%d"),
                **_profisee_metadata(self.rng, code, "Location"),
            }
            rows.append(row)
        return pd.DataFrame(rows)

    # ── 7. Employees / Contacts ───────────────────────────────────────────

    def _generate_employees(self, n: int = 150) -> pd.DataFrame:
        first_names = ["James", "Maria", "Robert", "Jennifer", "Carlos", "Sarah", "Michael", "Lisa",
                       "David", "Amanda", "Daniel", "Patricia", "Jose", "Emily", "Kevin", "Ashley"]
        last_names = ["Smith", "Johnson", "Williams", "Brown", "Garcia", "Martinez", "Davis", "Rodriguez",
                      "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Lee", "Harris"]
        titles = [
            "Pipeline Operator", "Field Technician", "Operations Manager",
            "Safety Specialist", "Environmental Compliance Officer",
            "SCADA Analyst", "Measurement Technician", "Integrity Engineer",
            "Corrosion Technician", "Gas Controller", "Plant Operator",
            "Area Supervisor", "Data Steward", "Regulatory Analyst",
        ]
        rows = []
        for i in range(1, n + 1):
            code = _profisee_code("EMP", i)
            first = self.rng.choice(first_names)
            last = self.rng.choice(last_names)
            row = {
                "employee_id": f"ET{self.rng.integers(100000, 999999)}",
                "first_name": first,
                "last_name": last,
                "email": f"{first.lower()}.{last.lower()}@energytransfer.com",
                "job_title": self.rng.choice(titles),
                "department": self.rng.choice(["Operations", "Engineering", "Safety", "IT", "Commercial", "Compliance", "Measurement"]),
                "work_location_state": self.rng.choice(STATES),
                "business_segment": self.rng.choice(BUSINESS_SEGMENTS),
                "hire_date": (datetime(2005, 1, 1) + timedelta(days=int(self.rng.integers(0, 7000)))).strftime("%Y-%m-%d"),
                "is_active": bool(self.rng.random() > 0.05),
                "operator_qualification_status": self.rng.choice(["Qualified", "In Training", "Expired", "N/A"]),
                **_profisee_metadata(self.rng, code, "Employee"),
            }
            rows.append(row)
        return pd.DataFrame(rows)

    # ── 8. Regulatory / Permits ───────────────────────────────────────────

    def _generate_regulatory(self, n: int = 100) -> pd.DataFrame:
        permit_types = [
            "FERC Certificate", "State Pipeline Permit", "Air Quality Permit",
            "NPDES Water Discharge", "SPCC Plan", "DOT Special Permit",
            "Army Corps 404 Permit", "Endangered Species Consultation",
            "Cultural Resources Survey", "HCA Identification",
        ]
        rows = []
        for i in range(1, n + 1):
            code = _profisee_code("REG", i)
            ptype = self.rng.choice(permit_types)
            state = self.rng.choice(STATES)
            issue_dt = datetime(2018, 1, 1) + timedelta(days=int(self.rng.integers(0, 2500)))
            expiry_dt = issue_dt + timedelta(days=int(self.rng.integers(365, 3650)))

            row = {
                "permit_id": f"{state}-{ptype[:4].upper()}-{self.rng.integers(10000, 99999)}",
                "permit_type": ptype,
                "issuing_agency": self.rng.choice(["FERC", "EPA", "DOT-PHMSA", "State DEQ", "Army Corps", "TCEQ", "Railroad Commission of Texas"]),
                "state": state,
                "associated_pipeline": self.rng.choice(PIPELINE_SYSTEMS),
                "issue_date": issue_dt.strftime("%Y-%m-%d"),
                "expiry_date": expiry_dt.strftime("%Y-%m-%d"),
                "status": self.rng.choice(["Active", "Pending Renewal", "Expired", "Under Review"]),
                "compliance_finding": self.rng.choice(["No Violations", "Minor Finding", "Corrective Action Required", None]),
                **_profisee_metadata(self.rng, code, "Regulatory"),
            }
            rows.append(row)
        return pd.DataFrame(rows)

    # ── 9. Contracts ──────────────────────────────────────────────────────

    def _generate_contracts(self, n: int = 200) -> pd.DataFrame:
        contract_types = [
            "Firm Transportation", "Interruptible Transportation",
            "Storage", "Processing", "Fractionation",
            "Throughput & Deficiency", "Gathering", "Terminalling",
        ]
        rows = []
        for i in range(1, n + 1):
            code = _profisee_code("CNTR", i)
            ctype = self.rng.choice(contract_types)
            start_dt = datetime(2020, 1, 1) + timedelta(days=int(self.rng.integers(0, 1500)))
            term_years = int(self.rng.choice([1, 2, 3, 5, 7, 10, 15, 20]))
            end_dt = start_dt + timedelta(days=term_years * 365)
            mdq = int(self.rng.integers(5_000, 2_000_000))

            row = {
                "contract_number": f"ET-{ctype[:3].upper()}-{self.rng.integers(100000, 999999)}",
                "contract_type": ctype,
                "customer_code": _profisee_code("CUST", int(self.rng.integers(1, 300))),
                "pipeline_system": self.rng.choice(PIPELINE_SYSTEMS),
                "commodity": self.rng.choice(PRODUCT_TYPES[:7]),
                "effective_date": start_dt.strftime("%Y-%m-%d"),
                "expiration_date": end_dt.strftime("%Y-%m-%d"),
                "term_years": term_years,
                "max_daily_quantity_mmbtu": mdq if "Gas" in ctype or "Transport" in ctype else None,
                "max_daily_quantity_bpd": mdq if "Gas" not in ctype else None,
                "rate_per_unit_usd": round(float(self.rng.uniform(0.05, 2.50)), 4),
                "has_escalation_clause": bool(self.rng.random() > 0.4),
                "auto_renew": bool(self.rng.random() > 0.5),
                "status": self.rng.choice(["Active", "Pending Execution", "Expired", "Terminated"]),
                **_profisee_metadata(self.rng, code, "Contract"),
            }
            rows.append(row)
        return pd.DataFrame(rows)

    # ── 10. Fact: Daily Throughput ─────────────────────────────────────────

    def _generate_throughput(self, n_days: int = 90, n_assets: int = 50) -> pd.DataFrame:
        """Simulate daily throughput measurements — what the bronze layer would see."""
        rows = []
        start_date = datetime(2025, 1, 1)
        for day_offset in range(n_days):
            meas_date = (start_date + timedelta(days=day_offset)).strftime("%Y-%m-%d")
            for asset_idx in range(1, n_assets + 1):
                pipeline_code = _profisee_code("PIPE", asset_idx)
                base_volume = float(self.rng.integers(50_000, 3_000_000))
                actual = base_volume * float(self.rng.uniform(0.7, 1.05))
                rows.append({
                    "measurement_date": meas_date,
                    "pipeline_asset_code": pipeline_code,
                    "scheduled_volume_mmbtu": round(base_volume, 0),
                    "actual_volume_mmbtu": round(actual, 0),
                    "utilization_pct": round(actual / base_volume * 100, 2),
                    "flow_direction": self.rng.choice(["Forward", "Reverse", "Bidirectional"]),
                    "measurement_quality_flag": self.rng.choice(["Validated", "Estimated", "Suspect"]),
                    "_profisee_export_batch_id": f"BATCH-{uuid.uuid4().hex[:12].upper()}",
                    "_profisee_updated_dtm": (start_date + timedelta(days=day_offset, hours=int(self.rng.integers(0, 23)))).isoformat(),
                })
        return pd.DataFrame(rows)

    # ── Orchestrator ──────────────────────────────────────────────────────

    def generate_all(self) -> dict[str, pd.DataFrame]:
        """Generate all master-data domains and return as a dict of DataFrames."""
        print("🔄 Generating Profisee MDM golden-record data for Energy Transfer…")
        self._tables = {
            "dim_pipeline_assets": self._generate_pipeline_assets(200),
            "dim_facilities": self._generate_facilities(120),
            "dim_customers": self._generate_customers(300),
            "dim_vendors": self._generate_vendors(180),
            "dim_products": self._generate_products(50),
            "dim_locations": self._generate_locations(250),
            "dim_employees": self._generate_employees(150),
            "dim_regulatory": self._generate_regulatory(100),
            "dim_contracts": self._generate_contracts(200),
            "fact_throughput_daily": self._generate_throughput(n_days=90, n_assets=50),
        }
        for name, df in self._tables.items():
            print(f"  ✅ {name:30s} → {len(df):>7,} rows  |  {len(df.columns)} columns")
        print(f"\n📊 Total: {sum(len(df) for df in self._tables.values()):,} records across {len(self._tables)} tables")
        return self._tables

    # ── Writers ────────────────────────────────────────────────────────────

    def write_parquet(self, output_dir: str = "./output") -> None:
        """Write all tables as Parquet files (for local testing)."""
        path = Path(output_dir)
        path.mkdir(parents=True, exist_ok=True)
        for name, df in self._tables.items():
            fpath = path / f"{name}.parquet"
            df.to_parquet(fpath, index=False, engine="pyarrow")
            print(f"  💾 {fpath}")

    def write_delta_tables(self, catalog: str = "mdm", schema: str = "energy_transfer") -> None:
        """
        Write all tables as Delta tables in Databricks Unity Catalog.
        ⚠️  Only works inside a Databricks Runtime with PySpark available.
        """
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
        except ImportError:
            print("⚠️  PySpark not available. Use write_parquet() for local testing.")
            return

        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

        for name, df in self._tables.items():
            full_table = f"{catalog}.{schema}.{name}"
            sdf = spark.createDataFrame(df)
            sdf.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(full_table)
            print(f"  ✅ Written → {full_table}  ({len(df):,} rows)")

    def get_databricks_notebook_code(self) -> str:
        """Return copy-pasteable notebook cells for Databricks."""
        return _NOTEBOOK_TEMPLATE


# ---------------------------------------------------------------------------
# Databricks notebook template
# ---------------------------------------------------------------------------

_NOTEBOOK_TEMPLATE = '''
# ─────────────────────────────────────────────────────────────────────────────
# CELL 1: Install & import
# ─────────────────────────────────────────────────────────────────────────────
# %pip install faker  # optional — for more realistic names

from profisee_energy_transfer_simulator import ProfiseeMDMSimulator

# ─────────────────────────────────────────────────────────────────────────────
# CELL 2: Generate all master data domains
# ─────────────────────────────────────────────────────────────────────────────
sim = ProfiseeMDMSimulator(seed=42)
tables = sim.generate_all()

# ─────────────────────────────────────────────────────────────────────────────
# CELL 3: Write to Unity Catalog as Delta tables
# ─────────────────────────────────────────────────────────────────────────────
sim.write_delta_tables(catalog="mdm", schema="energy_transfer")

# ─────────────────────────────────────────────────────────────────────────────
# CELL 4: Verify — query one table
# ─────────────────────────────────────────────────────────────────────────────
display(spark.table("mdm.energy_transfer.dim_pipeline_assets").limit(20))

# ─────────────────────────────────────────────────────────────────────────────
# CELL 5: Profisee metadata analysis — data quality distribution
# ─────────────────────────────────────────────────────────────────────────────
from pyspark.sql import functions as F

for table_name in [
    "dim_pipeline_assets", "dim_customers", "dim_facilities",
    "dim_vendors", "dim_contracts",
]:
    df = spark.table(f"mdm.energy_transfer.{table_name}")
    print(f"\\n{'='*60}")
    print(f"📊 {table_name}")
    print(f"{'='*60}")
    df.groupBy("_profisee_record_status").count().show()
    df.groupBy("_profisee_match_status").count().show()
    df.select(
        F.avg("_profisee_data_quality_score").alias("avg_dq"),
        F.min("_profisee_data_quality_score").alias("min_dq"),
        F.max("_profisee_data_quality_score").alias("max_dq"),
    ).show()

# ─────────────────────────────────────────────────────────────────────────────
# CELL 6: Simulate Lakeflow Declarative Pipeline (Silver Layer)
# ─────────────────────────────────────────────────────────────────────────────
# This SQL can be pasted into a Lakeflow Declarative Pipeline notebook:
#
# CREATE OR REFRESH STREAMING TABLE silver_pipeline_assets AS
# SELECT
#     pipeline_system,
#     segment_id,
#     business_segment,
#     diameter_inches,
#     length_miles,
#     commodity_transported,
#     state,
#     _profisee_data_quality_score,
#     _profisee_record_status,
#     _profisee_updated_dtm
# FROM STREAM(mdm.energy_transfer.dim_pipeline_assets)
# WHERE _profisee_record_status = 'Active'
#   AND _profisee_data_quality_score >= 70;
#
# CREATE OR REFRESH MATERIALIZED VIEW gold_pipeline_summary AS
# SELECT
#     business_segment,
#     commodity_transported,
#     COUNT(*) AS segment_count,
#     ROUND(SUM(length_miles), 1) AS total_miles,
#     ROUND(AVG(_profisee_data_quality_score), 2) AS avg_dq_score
# FROM silver_pipeline_assets
# GROUP BY business_segment, commodity_transported;
'''


# ---------------------------------------------------------------------------
# CLI entry-point — run locally for quick validation
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    sim = ProfiseeMDMSimulator(seed=42)
    tables = sim.generate_all()
    sim.write_delta_tables(catalog="mdm", schema="energy_transfer")

    # Print sample from one table
    print("\n" + "=" * 80)
    print("SAMPLE: dim_pipeline_assets (first 3 rows, selected columns)")
    print("=" * 80)
    sample_cols = [
        "pipeline_system", "segment_id", "commodity_transported",
        "diameter_inches", "length_miles", "state",
        "_profisee_code", "_profisee_record_status",
        "_profisee_match_status", "_profisee_data_quality_score",
    ]
    print(tables["dim_pipeline_assets"][sample_cols].head(3).to_string(index=False))
