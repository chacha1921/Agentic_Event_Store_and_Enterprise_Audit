"""datagen/pdf_generator.py — GAAP financial statement PDFs + Application Proposal"""
from __future__ import annotations
import random
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.colors import HexColor
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer

NAVY = HexColor('#1E3A5F')
LIGHT = HexColor('#EBF5FB')

def _m(v): return f"${v:,.0f}" if v and abs(v) >= 1 else ("—" if v is None else f"${v:.2f}")
def _neg(v): return f"(${abs(v):,.0f})" if v and v < 0 else _m(v)

TS_BASE = TableStyle([
    ('FONTNAME',(0,0),(-1,0),'Helvetica-Bold'),
    ('BACKGROUND',(0,0),(-1,0),NAVY),
    ('TEXTCOLOR',(0,0),(-1,0),colors.white),
    ('FONTSIZE',(0,0),(-1,-1),10),
    ('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.white,LIGHT]),
    ('GRID',(0,0),(-1,-1),0.5,colors.lightgrey),
    ('ALIGN',(1,0),(1,-1),'RIGHT'),
])

def _doc(path, title, subtitle):
    styles = getSampleStyleSheet()
    doc = SimpleDocTemplate(path, pagesize=letter,
                            topMargin=0.75*inch, bottomMargin=0.75*inch,
                            leftMargin=inch, rightMargin=inch)
    elems = [
        Paragraph(f"<b>{title}</b>", ParagraphStyle('t',fontName='Helvetica-Bold',fontSize=14,spaceAfter=4)),
        Paragraph(subtitle, ParagraphStyle('s',fontName='Helvetica',fontSize=10,spaceAfter=4,textColor=colors.HexColor('#5D6D7E'))),
        Paragraph("(All amounts in USD — prepared in accordance with US GAAP)",
                  ParagraphStyle('g',fontName='Helvetica-Oblique',fontSize=8,spaceAfter=10,textColor=colors.gray)),
    ]
    return doc, elems

def generate_income_statement_pdf(company, fiscal_year: int, output_path: str,
                                   variant: str = "clean") -> None:
    fin = next(f for f in company.financials if f["fiscal_year"] == fiscal_year)
    doc, elems = _doc(output_path, company.name,
                      f"Consolidated Income Statement — Fiscal Year Ended December 31, {fiscal_year}")
    if variant == "missing_ebitda":
        rows = [["Line Item", f"FY{fiscal_year}"],
                ["Revenue", _m(fin["total_revenue"])],
                ["Cost of Revenue", _neg(-(fin["total_revenue"]-fin["gross_profit"]))],
                ["Gross Profit", _m(fin["gross_profit"])],
                ["Operating Expenses", _neg(-fin["operating_expenses"])],
                ["Depreciation & Amortization", _neg(-fin["depreciation_amortization"])],
                ["Operating Income (EBIT)", _m(fin["operating_income"])],
                # EBITDA deliberately absent
                ["Interest Expense", _neg(-fin["interest_expense"])],
                ["Income Before Tax", _m(fin["income_before_tax"])],
                ["Income Tax Expense", _neg(-fin["tax_expense"])],
                ["Net Income", _m(fin["net_income"])]]
    elif variant == "dense":
        prev = company.financials[1] if len(company.financials) > 1 else fin
        rows = [["Line Item", f"FY{fiscal_year}", f"FY{fiscal_year-1}"],
                ["Net Revenue", _m(fin["total_revenue"]), _m(prev["total_revenue"])],
                ["  Direct Materials", _neg(-fin["total_revenue"]*0.32), "—"],
                ["  Direct Labor", _neg(-fin["total_revenue"]*0.15), "—"],
                ["  Manufacturing Overhead", _neg(-fin["total_revenue"]*0.10), "—"],
                ["COGS", _neg(-(fin["total_revenue"]-fin["gross_profit"])), "—"],
                ["Gross Profit", _m(fin["gross_profit"]), "—"],
                ["  SG&A", _neg(-fin["operating_expenses"]*0.6), "—"],
                ["  R&D", _neg(-fin["operating_expenses"]*0.2), "—"],
                ["  Other OpEx", _neg(-fin["operating_expenses"]*0.2), "—"],
                ["Operating Income (EBIT)", _m(fin["operating_income"]), "—"],
                ["EBITDA", _m(fin["ebitda"]), "—"],
                ["D&A", _neg(-fin["depreciation_amortization"]), "—"],
                ["Interest Expense", _neg(-fin["interest_expense"]), "—"],
                ["Income Before Tax", _m(fin["income_before_tax"]), "—"],
                ["Income Tax", _neg(-fin["tax_expense"]), "—"],
                ["Net Income", _m(fin["net_income"]), "—"]]
        cw = [3.0*inch, 1.8*inch, 1.8*inch]
        t = Table(rows, colWidths=cw); t.setStyle(TS_BASE); elems.append(t)
        doc.build(elems); return
    else:
        rows = [["Line Item", f"FY{fiscal_year}"],
                ["Revenue", _m(fin["total_revenue"])],
                ["Cost of Revenue", _neg(-(fin["total_revenue"]-fin["gross_profit"]))],
                ["Gross Profit", _m(fin["gross_profit"])],
                ["  Gross Margin %", f"{fin['gross_margin']*100:.1f}%"],
                ["Operating Expenses", _neg(-fin["operating_expenses"])],
                ["Depreciation & Amortization", _neg(-fin["depreciation_amortization"])],
                ["Operating Income (EBIT)", _m(fin["operating_income"])],
                ["EBITDA", _m(fin["ebitda"])],
                ["  EBITDA Margin %", f"{fin['ebitda_margin']*100:.1f}%"],
                ["Interest Expense", _neg(-fin["interest_expense"])],
                ["Income Before Tax", _m(fin["income_before_tax"])],
                ["Income Tax Expense", _neg(-fin["tax_expense"])],
                ["Net Income", _m(fin["net_income"])],
                ["  Net Margin %", f"{fin['net_margin']*100:.1f}%"]]
    t = Table(rows, colWidths=[3.8*inch, 2.0*inch]); t.setStyle(TS_BASE)
    elems.append(t)
    if variant == "scanned":
        elems.append(Spacer(1,0.2*inch))
        elems.append(Paragraph("Note: Reproduced from original physical records. Some figures may reflect manual transcription.",
                                ParagraphStyle('n',fontName='Helvetica-Oblique',fontSize=8,textColor=colors.gray)))
    doc.build(elems)

def generate_balance_sheet_pdf(company, fiscal_year: int, output_path: str,
                                variant: str = "clean") -> None:
    fin = next(f for f in company.financials if f["fiscal_year"] == fiscal_year)
    doc, elems = _doc(output_path, company.name,
                      f"Consolidated Balance Sheet — As of December 31, {fiscal_year}")
    disc = random.uniform(500, 4500) if random.random() < 0.06 else 0
    eq_disp = fin["total_equity"] + disc
    rows = [["Line Item", f"Dec 31, {fiscal_year}"],
            ["ASSETS", ""],
            ["  Cash and Cash Equivalents", _m(fin["cash_and_equivalents"])],
            ["  Accounts Receivable, net", _m(fin["accounts_receivable"])],
            ["  Inventory", _m(fin["inventory"])],
            ["  Other Current Assets", _m(max(fin["current_assets"]-fin["cash_and_equivalents"]-fin["accounts_receivable"]-fin["inventory"],0))],
            ["Total Current Assets", _m(fin["current_assets"])],
            ["  Property, Plant & Equipment, net", _m(fin["total_assets"]-fin["current_assets"])],
            ["TOTAL ASSETS", _m(fin["total_assets"])],
            ["", ""],
            ["LIABILITIES & EQUITY", ""],
            ["  Accounts Payable", _m(fin["current_liabilities"]*0.45)],
            ["  Accrued Liabilities", _m(fin["current_liabilities"]*0.30)],
            ["  Current Portion LTD", _m(fin["current_liabilities"]*0.25)],
            ["Total Current Liabilities", _m(fin["current_liabilities"])],
            ["  Long-Term Debt", _m(fin["long_term_debt"])],
            ["  Other Long-Term Liabilities", _m(max(fin["total_liabilities"]-fin["current_liabilities"]-fin["long_term_debt"],0))],
            ["TOTAL LIABILITIES", _m(fin["total_liabilities"])],
            ["  Members' / Stockholders' Equity", _m(eq_disp)],
            ["TOTAL EQUITY", _m(eq_disp)],
            ["TOTAL LIABILITIES & EQUITY", _m(fin["total_liabilities"]+eq_disp)]]
    t = Table(rows, colWidths=[3.8*inch, 2.0*inch])
    t.setStyle(TS_BASE)
    for i, row in enumerate(rows):
        if row[0].startswith("TOTAL") or row[0] in ("ASSETS","LIABILITIES & EQUITY"):
            t.setStyle(TableStyle([('FONTNAME',(0,i),(-1,i),'Helvetica-Bold')]))
    elems.append(t)
    if disc > 0:
        elems.append(Spacer(1,0.15*inch))
        elems.append(Paragraph(f"Note: Immaterial rounding adjustment of ${disc:,.0f} applied.",
                                ParagraphStyle('n',fontName='Helvetica-Oblique',fontSize=8,textColor=colors.gray)))
    doc.build(elems)

def generate_application_proposal_pdf(company, application_id: str,
                                       requested_amount: float, loan_purpose: str,
                                       output_path: str) -> None:
    fin = company.financials[-1]
    doc, elems = _doc(output_path, company.name, f"Commercial Loan Application — {application_id}")
    PURPOSE_DESC = {
        "working_capital": "fund day-to-day operational expenses and seasonal cash flow",
        "equipment_financing": "acquire critical production equipment",
        "expansion": "open new facility and hire staff to expand market presence",
        "real_estate": "acquire commercial real estate for primary operations",
        "refinancing": "consolidate existing debt at improved terms",
        "acquisition": "acquire a complementary business",
        "bridge": "bridge a temporary financing gap pending longer-term arrangement",
    }
    # Entity info table
    info = [["Legal Entity Name:", company.name],["Business Type:", company.legal_type],
            ["Industry:", company.industry.replace("_"," ").title()],["NAICS:", company.naics],
            ["EIN:", company.ein],["Jurisdiction:", company.jurisdiction],
            ["Founded:", str(company.founded_year)],["Employees:", str(company.employee_count)]]
    it = Table(info, colWidths=[2.5*inch, 3.5*inch])
    it.setStyle(TableStyle([('FONTNAME',(0,0),(0,-1),'Helvetica-Bold'),('FONTSIZE',(0,0),(-1,-1),10),
                             ('ROWBACKGROUNDS',(0,0),(-1,-1),[colors.white,LIGHT]),
                             ('GRID',(0,0),(-1,-1),0.5,colors.lightgrey),('PADDING',(0,0),(-1,-1),6)]))
    elems.extend([it, Spacer(1,0.2*inch),
                  Paragraph("<b>Loan Request</b>", ParagraphStyle('h',fontName='Helvetica-Bold',fontSize=12,spaceAfter=6,textColor=NAVY))])
    loan = [["Requested Amount:", f"${requested_amount:,.0f}"],
            ["Purpose:", loan_purpose.replace("_"," ").title()],
            ["Use of Proceeds:", PURPOSE_DESC.get(loan_purpose,"general business purposes")]]
    lt = Table(loan, colWidths=[2.5*inch, 3.5*inch])
    lt.setStyle(TableStyle([('FONTNAME',(0,0),(0,-1),'Helvetica-Bold'),('FONTSIZE',(0,0),(-1,-1),10),
                             ('GRID',(0,0),(-1,-1),0.5,colors.lightgrey),('PADDING',(0,0),(-1,-1),6)]))
    elems.extend([lt, Spacer(1,0.2*inch),
                  Paragraph("<b>Financial Highlights (FY2024)</b>",ParagraphStyle('h2',fontName='Helvetica-Bold',fontSize=12,spaceAfter=6,textColor=NAVY))])
    prev = company.financials[1]
    hl = [["Metric","FY2024","FY2023"],
          ["Revenue", _m(fin["total_revenue"]), _m(prev["total_revenue"])],
          ["EBITDA", _m(fin["ebitda"]), _m(prev["ebitda"])],
          ["Net Income", _m(fin["net_income"]), _m(prev["net_income"])],
          ["Total Assets", _m(fin["total_assets"]), "—"],
          ["Debt/EBITDA", f"{fin['debt_to_ebitda']:.2f}x", "—"],
          ["Current Ratio", f"{fin['current_ratio']:.2f}x", "—"]]
    ht = Table(hl, colWidths=[2.5*inch,1.6*inch,1.6*inch])
    ht.setStyle(TableStyle([('FONTNAME',(0,0),(-1,0),'Helvetica-Bold'),('BACKGROUND',(0,0),(-1,0),NAVY),
                             ('TEXTCOLOR',(0,0),(-1,0),colors.white),('FONTSIZE',(0,0),(-1,-1),10),
                             ('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.white,LIGHT]),
                             ('GRID',(0,0),(-1,-1),0.5,colors.lightgrey),('ALIGN',(1,0),(-1,-1),'RIGHT'),('PADDING',(0,0),(-1,-1),6)]))
    elems.append(ht)
    doc.build(elems)
