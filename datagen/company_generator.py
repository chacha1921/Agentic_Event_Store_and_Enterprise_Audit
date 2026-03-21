"""datagen/company_generator.py — company profiles + 3yr GAAP financials"""
from __future__ import annotations
import random
from dataclasses import dataclass, field
from faker import Faker

fake = Faker()

INDUSTRIES = {
    "logistics":           {"count":12,"revenue_range":(800_000,14_000_000),"gross_margin":(0.18,0.32),"ebitda_margin":(0.06,0.14),"asset_mult":(1.6,2.4),"debt_ratio":(0.40,0.60),"naics":"484110","purposes":["working_capital","equipment_financing","expansion"]},
    "manufacturing":       {"count":10,"revenue_range":(1_200_000,22_000_000),"gross_margin":(0.22,0.40),"ebitda_margin":(0.08,0.18),"asset_mult":(2.0,3.2),"debt_ratio":(0.45,0.65),"naics":"332710","purposes":["equipment_financing","working_capital","expansion"]},
    "technology":          {"count":10,"revenue_range":(500_000,9_000_000),"gross_margin":(0.55,0.80),"ebitda_margin":(0.10,0.28),"asset_mult":(1.1,1.8),"debt_ratio":(0.20,0.40),"naics":"541511","purposes":["working_capital","expansion","acquisition"]},
    "healthcare":          {"count":9,"revenue_range":(700_000,18_000_000),"gross_margin":(0.30,0.55),"ebitda_margin":(0.08,0.20),"asset_mult":(1.5,2.8),"debt_ratio":(0.35,0.55),"naics":"621111","purposes":["equipment_financing","expansion","real_estate"]},
    "retail":              {"count":9,"revenue_range":(600_000,11_000_000),"gross_margin":(0.25,0.45),"ebitda_margin":(0.05,0.12),"asset_mult":(1.2,1.9),"debt_ratio":(0.50,0.70),"naics":"441110","purposes":["working_capital","expansion","refinancing"]},
    "professional_services":{"count":9,"revenue_range":(400_000,7_000_000),"gross_margin":(0.50,0.75),"ebitda_margin":(0.12,0.30),"asset_mult":(0.9,1.5),"debt_ratio":(0.20,0.38),"naics":"541200","purposes":["working_capital","acquisition","expansion"]},
    "construction":        {"count":9,"revenue_range":(1_000_000,20_000_000),"gross_margin":(0.15,0.28),"ebitda_margin":(0.05,0.12),"asset_mult":(1.7,2.8),"debt_ratio":(0.50,0.72),"naics":"236220","purposes":["equipment_financing","working_capital","bridge"]},
    "other":               {"count":12,"revenue_range":(500_000,8_000_000),"gross_margin":(0.20,0.50),"ebitda_margin":(0.06,0.18),"asset_mult":(1.2,2.0),"debt_ratio":(0.30,0.55),"naics":"519130","purposes":["working_capital","expansion"]},
}
TRAJECTORIES = {"GROWTH":20,"STABLE":25,"DECLINING":12,"RECOVERING":13,"VOLATILE":10}
TRAJECTORY_YOY = {"GROWTH":(0.12,0.22),"STABLE":(0.02,0.07),"DECLINING":(-0.10,-0.02),"RECOVERING":(-0.08,0.18),"VOLATILE":(-0.12,0.20)}
US_STATES = ["CA","TX","NY","FL","IL","PA","OH","GA","NC","MI","NJ","VA","WA","AZ","MA","TN","IN","MO","MD","WI","CO","MN","SC","AL","LA","KY","OR","OK","CT","UT","NV","AR","MS","KS","NE","ID","HI","NH","ME","RI","DE","SD","ND","AK","VT","WY"]
LEGAL_TYPES = ["LLC","LLC","LLC","Corp","Corp","S-Corp","Partnership","Sole Proprietor"]

@dataclass
class GeneratedCompany:
    company_id: str
    name: str
    industry: str
    naics: str
    jurisdiction: str
    legal_type: str
    founded_year: int
    employee_count: int
    ein: str
    address_city: str
    address_state: str
    relationship_start: str
    account_manager: str
    risk_segment: str
    trajectory: str
    financials: list[dict]
    loan_purposes: list[str]
    submission_channel: str
    ip_region: str
    compliance_flags: list[dict] = field(default_factory=list)

def _r2(v): return round(v, 2)

def generate_gaap_financials(industry: str, trajectory: str, base_revenue: float) -> list[dict]:
    p = INDUSTRIES[industry]
    results = []
    rev = base_revenue
    for i, year in enumerate([2022, 2023, 2024]):
        if i > 0:
            if trajectory == "RECOVERING":
                yoy = -0.07 if i == 1 else random.uniform(0.08, 0.20)
            else:
                yoy = random.uniform(*TRAJECTORY_YOY[trajectory])
            rev = max(results[-1]["total_revenue"] * (1 + yoy), 100_000)
        gm = random.uniform(*p["gross_margin"])
        em = random.uniform(*p["ebitda_margin"])
        gross_profit = rev * gm
        ebitda = rev * em
        da = rev * random.uniform(0.02, 0.05)
        op_inc = ebitda - da
        opex = gross_profit - op_inc
        ltd = rev * random.uniform(0.30, 0.70)
        int_exp = ltd * random.uniform(0.05, 0.09)
        ebt = op_inc - int_exp
        tax = max(ebt * random.uniform(0.21, 0.28), 0) if ebt > 0 else 0
        net_inc = ebt - tax
        am = random.uniform(*p["asset_mult"])
        total_assets = rev * am
        cur_assets = total_assets * random.uniform(0.30, 0.55)
        cash = cur_assets * random.uniform(0.15, 0.35)
        ar = cur_assets * random.uniform(0.25, 0.45)
        inv = max((cur_assets - cash - ar) * random.uniform(0.5, 1.0), 0)
        cur_liab = cur_assets / random.uniform(1.3, 2.8)
        total_liab = total_assets * random.uniform(*p["debt_ratio"])
        lt_liab = max(total_liab - cur_liab, 0)
        total_liab = cur_liab + lt_liab
        equity = total_assets - total_liab
        op_cf = net_inc + da + random.uniform(-0.05, 0.05) * rev
        inv_cf = -(rev * random.uniform(0.03, 0.10))
        fin_cf = -(ltd * random.uniform(0.05, 0.15))
        results.append({
            "fiscal_year": year,
            "total_revenue": _r2(rev), "gross_profit": _r2(gross_profit),
            "operating_expenses": _r2(opex), "operating_income": _r2(op_inc),
            "ebitda": _r2(ebitda), "depreciation_amortization": _r2(da),
            "interest_expense": _r2(int_exp), "income_before_tax": _r2(ebt),
            "tax_expense": _r2(tax), "net_income": _r2(net_inc),
            "total_assets": _r2(total_assets), "current_assets": _r2(cur_assets),
            "cash_and_equivalents": _r2(cash), "accounts_receivable": _r2(ar),
            "inventory": _r2(inv), "total_liabilities": _r2(total_liab),
            "current_liabilities": _r2(cur_liab), "long_term_debt": _r2(ltd),
            "total_equity": _r2(equity),
            "operating_cash_flow": _r2(op_cf), "investing_cash_flow": _r2(inv_cf),
            "financing_cash_flow": _r2(fin_cf), "free_cash_flow": _r2(op_cf + inv_cf),
            "debt_to_equity": _r2(total_liab / max(equity, 1)),
            "current_ratio": _r2(cur_assets / max(cur_liab, 1)),
            "debt_to_ebitda": _r2(total_liab / max(ebitda, 1)),
            "interest_coverage_ratio": _r2(op_inc / max(int_exp, 1)),
            "gross_margin": _r2(gm), "ebitda_margin": _r2(em),
            "net_margin": _r2(net_inc / max(rev, 1)),
            "balance_sheet_check": abs(total_assets - total_liab - equity) < 1.0,
        })
    return results

def generate_companies(n: int = 80) -> list[GeneratedCompany]:
    traj_list = []
    for t, cnt in TRAJECTORIES.items():
        traj_list.extend([t] * cnt)
    random.shuffle(traj_list)
    ind_list = []
    for ind, p in INDUSTRIES.items():
        ind_list.extend([ind] * p["count"])
    random.shuffle(ind_list)
    # Ensure one Montana company for REG-003 compliance test
    mt_idx = min(random.randint(10, max(10, n-2)), n-1)
    companies = []
    for i in range(n):
        industry = ind_list[i] if i < len(ind_list) else "other"
        trajectory = traj_list[i] if i < len(traj_list) else "STABLE"
        p = INDUSTRIES[industry]
        jurisdiction = "MT" if i == mt_idx else random.choice(US_STATES)
        legal_type = random.choice(LEGAL_TYPES)
        founded_year = random.randint(2000, 2020)
        base_rev = random.uniform(*p["revenue_range"])
        financials = generate_gaap_financials(industry, trajectory, base_rev)
        latest = financials[-1]
        flags = []
        if random.random() < 0.10:
            ft = random.choice(["AML_WATCH","SANCTIONS_REVIEW","PEP_LINK"])
            flags.append({"flag_type":ft,"severity":"HIGH" if ft=="SANCTIONS_REVIEW" else "MEDIUM","is_active":random.random()<0.6,"added_date":fake.date_between(start_date="-2y",end_date="today").isoformat(),"note":fake.sentence()})
        dr = latest["total_liabilities"] / max(latest["total_assets"], 1)
        has_default = random.random() < 0.08
        if has_default or (flags and any(f["is_active"] for f in flags)):
            risk = "HIGH"
        elif trajectory in ("GROWTH","STABLE") and dr < 0.40:
            risk = "LOW"
        else:
            risk = "MEDIUM"
        companies.append(GeneratedCompany(
            company_id=f"COMP-{i+1:03d}", name=fake.company().replace(",",""),
            industry=industry, naics=p["naics"], jurisdiction=jurisdiction,
            legal_type=legal_type, founded_year=founded_year,
            employee_count=random.randint(5,500),
            ein=f"{random.randint(10,99)}-{random.randint(1000000,9999999)}",
            address_city=fake.city(), address_state=jurisdiction,
            relationship_start=fake.date_between(start_date=f"-{min(2024-founded_year,10)}y",end_date="-6m").isoformat(),
            account_manager=fake.name(), risk_segment=risk,
            trajectory=trajectory, financials=financials,
            loan_purposes=p["purposes"],
            submission_channel=random.choice(["web","web","web","mobile","agent","branch"]),
            ip_region=random.choice(["US-East","US-West","US-Central","US-South"]),
            compliance_flags=flags,
        ))
    return companies
