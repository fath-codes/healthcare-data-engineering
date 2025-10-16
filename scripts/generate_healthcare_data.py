"""
Healthcare Synthetic Data Generator
Author: Ahmad Fathannafi (Practice Project)
Goal: Generate realistic synthetic healthcare data for ETL & Power BI visualization
Note: Uses Faker for realism; no external data required
"""

import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
from pathlib import Path

# -------------------------------------------------------------------
# Setup & Configuration
# -------------------------------------------------------------------
fake = Faker("id_ID")
np.random.seed(42)
random.seed(42)

RAW_DIR = Path("raw_data")
RAW_DIR.mkdir(exist_ok=True)

START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2025, 9, 30)

# -------------------------------------------------------------------
# 1. Departments
# -------------------------------------------------------------------
departments = [
    "Internal Medicine", "Surgery", "Pediatrics", "Obstetrics & Gynecology",
    "Cardiology", "Neurology", "Orthopedics", "Dermatology",
    "ENT (Ear, Nose, Throat)", "Emergency Department (IGD)"
]
dept_df = pd.DataFrame({
    "department_id": range(1, len(departments) + 1),
    "department_name": departments,
    "floor_number": [2, 3, 2, 3, 4, 4, 5, 2, 3, 0],
    "head_doctor": [f"Dr. {fake.name()}" for _ in departments]
})
dept_df.to_csv(RAW_DIR / "departments.csv", index=False)

# -------------------------------------------------------------------
# 2. Doctors
# -------------------------------------------------------------------
specialties = departments
doctor_rows = []
for i in range(1, 201):
    dept = random.choice(departments)
    doctor_rows.append({
        "doctor_id": i,
        "doctor_name": f"Dr. {fake.name()}",
        "department_name": dept,
        "specialization": dept,
        "years_experience": np.random.randint(2, 30),
        "status": random.choices(["Active", "On Leave"], weights=[0.9, 0.1])[0]
    })
doctor_df = pd.DataFrame(doctor_rows)
doctor_df.to_csv(RAW_DIR / "doctors.csv", index=False)

# -------------------------------------------------------------------
# 3. Patients
# -------------------------------------------------------------------
insurance_types = ["BPJS", "Private", "None"]
cities = ["Jakarta", "Bandung", "Surabaya", "Semarang", "Yogyakarta", "Medan"]

patient_rows = []
for i in range(1, 20001):
    age = np.random.randint(1, 90)
    patient_rows.append({
        "patient_id": i,
        "patient_name": fake.name(),
        "gender": random.choice(["Male", "Female"]),
        "age": age,
        "city": random.choice(cities),
        "insurance_type": random.choices(insurance_types, weights=[0.6, 0.3, 0.1])[0]
    })
patient_df = pd.DataFrame(patient_rows)
patient_df.to_csv(RAW_DIR / "patients.csv", index=False)

# -------------------------------------------------------------------
# 4. Diagnoses
# -------------------------------------------------------------------
diagnoses = [
    ("I10", "Hypertension"),
    ("E11", "Type 2 Diabetes Mellitus"),
    ("J06", "Upper Respiratory Infection"),
    ("K21", "Gastroesophageal Reflux Disease"),
    ("M54", "Back Pain"),
    ("N39", "Urinary Tract Infection"),
    ("F41", "Anxiety Disorder"),
    ("I25", "Ischemic Heart Disease"),
    ("A09", "Gastroenteritis"),
    ("L30", "Dermatitis"),
    ("G43", "Migraine"),
    ("H65", "Otitis Media"),
    ("M17", "Knee Osteoarthritis"),
    ("O80", "Normal Delivery"),
    ("S82", "Fracture of Lower Leg")
]
diag_df = pd.DataFrame(diagnoses, columns=["diagnosis_code", "diagnosis_name"])
diag_df["diagnosis_id"] = diag_df.index + 1
diag_df.to_csv(RAW_DIR / "diagnoses.csv", index=False)

# -------------------------------------------------------------------
# 5. Date Dimension
# -------------------------------------------------------------------
date_range = pd.date_range(start=START_DATE, end=END_DATE, freq="D")
date_df = pd.DataFrame({
    "date_id": range(1, len(date_range) + 1),
    "date": date_range,
})
date_df["year"] = date_df["date"].dt.year
date_df["month"] = date_df["date"].dt.month
date_df["day"] = date_df["date"].dt.day
date_df["quarter"] = date_df["date"].dt.quarter
date_df["day_name"] = date_df["date"].dt.day_name()
date_df.to_csv(RAW_DIR / "dates.csv", index=False)

# -------------------------------------------------------------------
# 6. Visits (Fact Table)
# -------------------------------------------------------------------
visit_rows = []
for i in range(1, 150001):
    patient = np.random.randint(1, 20001)
    doctor = np.random.randint(1, 201)
    dept = doctor_df.loc[doctor_df["doctor_id"]
                         == doctor, "department_name"].values[0]
    diag = random.choice(diagnoses)
    date = fake.date_between_dates(date_start=START_DATE, date_end=END_DATE)

    cost = np.random.lognormal(mean=13, sigma=0.5)
    cost = np.clip(cost, 300000, 10000000)
    insurance_cover = np.round(cost * random.uniform(0.3, 0.9), -3)
    patient_payment = np.round(cost - insurance_cover, -3)

    visit_rows.append({
        "visit_id": i,
        "patient_id": patient,
        "doctor_id": doctor,
        "department_name": dept,
        "diagnosis_code": diag[0],
        "visit_date": date,
        "visit_type": random.choices(["Rawat Jalan", "Rawat Inap", "IGD"], weights=[0.7, 0.25, 0.05])[0],
        "visit_duration_days": np.round(np.random.exponential(scale=2.5), 1),
        "total_cost": cost,
        "insurance_coverage": insurance_cover,
        "patient_payment": patient_payment,
        "satisfaction_rating": np.random.randint(1, 6)
    })

visit_df = pd.DataFrame(visit_rows)
visit_df.to_csv(RAW_DIR / "visits.csv", index=False)

print("✅ Synthetic Healthcare Data Generated Successfully!")
