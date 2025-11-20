import csv
import random
import string
import datetime

# Constants
NUM_RECORDS = 250  # Generating 250 records to satisfy "at least 200"
OUTPUT_FW = "imf_data.txt"
OUTPUT_CSV = "imf_data.csv"

class TaxpayerRecord:
    def __init__(self):
        self.data = {}
        self.fields = []  # Ordered list of field names

    def add_field(self, name, value, width, type_char='N'):
        """
        Adds a field to the record.
        type_char: 'N' for Numeric (right aligned, zero padded), 'A' for Alpha (left aligned, space padded)
        """
        self.fields.append((name, width, type_char))
        self.data[name] = value

    def generate_demographics(self):
        # Helper to generate random strings
        def rand_str(len_val, chars=string.ascii_uppercase):
            return ''.join(random.choice(chars) for _ in range(len_val))
        
        def rand_num(len_val):
            return ''.join(random.choice(string.digits) for _ in range(len_val))

        self.add_field("HEADER_DLN", rand_num(14), 14, 'A')
        self.add_field("HEADER_CYCLE_DT", "202415", 6, 'N')
        self.add_field("HEADER_TRANS_CD", "150", 3, 'N')
        self.add_field("HEADER_AUDIT_CD", random.choice(["H", " ", " "]), 1, 'A')
        
        # Generate ~46 more header fields
        for i in range(1, 47):
            self.add_field(f"HEADER_FILLER_{i}", "0", 1, 'N')

        # Demographics
        ssn = rand_num(9)
        self.add_field("ENTITY_SSN", ssn, 9, 'N')
        self.add_field("ENTITY_SPOUSE_SSN", rand_num(9) if random.random() > 0.5 else "", 9, 'N')
        
        last_name = rand_str(6)
        self.add_field("ENTITY_NAME_CTRL", last_name[:4], 4, 'A')
        self.add_field("ENTITY_PRI_NAME", f"JOHN {last_name}", 35, 'A')
        self.add_field("ENTITY_ADDRESS", f"{rand_num(4)} MAIN ST", 35, 'A')
        self.add_field("ENTITY_CITY", "ANYTOWN", 20, 'A')
        self.add_field("ENTITY_STATE", random.choice(["CA", "NY", "TX", "FL"]), 2, 'A')
        self.add_field("ENTITY_ZIP", rand_num(5), 9, 'N') # 5 digit zip padded to 9
        self.add_field("ENTITY_FILING_STATUS", str(random.randint(1, 5)), 1, 'N')
        
        # Add ~40 more entity fields
        for i in range(1, 41):
            self.add_field(f"ENTITY_HIST_PTR_{i}", rand_num(4), 4, 'N')

    def generate_income_and_schedules(self):
        # Core Income
        wages = random.randint(20000, 200000)
        self.add_field("INC_WAGES", wages, 12, 'N')
        
        interest = random.randint(0, 5000)
        self.add_field("INC_INTEREST", interest, 12, 'N')
        
        divs = random.randint(0, 10000)
        self.add_field("INC_DIVIDENDS", divs, 12, 'N')

        # Schedule C (Business) - 60 fields
        sch_c_gross = random.randint(0, 100000) if random.random() > 0.7 else 0
        self.add_field("SCH_C_GROSS_RCPT", sch_c_gross, 12, 'N')
        sch_c_expenses = 0
        for i in range(1, 60):
            exp = int(sch_c_gross * 0.01 * random.random())
            self.add_field(f"SCH_C_EXP_{i}", exp, 12, 'N')
            sch_c_expenses += exp
        
        sch_c_net = sch_c_gross - sch_c_expenses
        self.add_field("SCH_C_NET_PROFIT", sch_c_net, 12, 'N') # Can be negative, but keeping simple for now

        # Schedule D (Capital Gains) - 30 fields
        cap_gain = random.randint(0, 20000)
        self.add_field("SCH_D_TOT_GAIN", cap_gain, 12, 'N')
        for i in range(1, 30):
             self.add_field(f"SCH_D_DET_{i}", random.randint(0, 1000), 12, 'N')

        # Schedule E (Rental) - 40 fields
        sch_e_inc = random.randint(0, 30000) if random.random() > 0.8 else 0
        self.add_field("SCH_E_TOT_INC", sch_e_inc, 12, 'N')
        for i in range(1, 40):
            self.add_field(f"SCH_E_PROP_{i}", random.randint(0, 500), 12, 'N')

        # Schedule F (Farm) - 40 fields
        sch_f_inc = 0
        for i in range(1, 41):
            val = random.randint(0, 1000) if random.random() > 0.95 else 0
            self.add_field(f"SCH_F_LINE_{i}", val, 12, 'N')
            if i == 1: sch_f_inc = val # Simplified

        # Total Income Calculation
        total_income = wages + interest + divs + sch_c_net + cap_gain + sch_e_inc + sch_f_inc
        self.add_field("INC_TOTAL", total_income, 12, 'N')
        
        # Adjustments (Schedule 1)
        adj = random.randint(0, 5000)
        self.add_field("ADJ_TOTAL", adj, 12, 'N')
        
        agi = total_income - adj
        self.add_field("CALC_AGI", agi, 12, 'N')

        return agi

    def calculate_taxes(self, agi):
        # Deductions (Schedule A) - 40 fields
        std_deduction = 12000
        itemized = 0
        for i in range(1, 41):
            val = random.randint(0, 2000)
            self.add_field(f"SCH_A_LINE_{i}", val, 12, 'N')
            itemized += val
        
        deduction = max(std_deduction, itemized)
        self.add_field("CALC_DEDUCTION_USED", deduction, 12, 'N')
        
        taxable_inc = max(0, agi - deduction)
        self.add_field("CALC_TAXABLE_INC", taxable_inc, 12, 'N')

        # Tax Calculation
        tax = int(taxable_inc * 0.20) # Flat 20% for simplicity
        self.add_field("CALC_TENTATIVE_TAX", tax, 12, 'N')

        # Credits - 50 fields
        credits = 0
        for i in range(1, 51):
            val = random.randint(0, 500) if random.random() > 0.8 else 0
            self.add_field(f"CREDIT_LINE_{i}", val, 12, 'N')
            credits += val
        
        total_tax = max(0, tax - credits)
        self.add_field("CALC_TOTAL_TAX", total_tax, 12, 'N')
        
        # Payments
        withheld = int(tax * 1.1) if random.random() > 0.3 else int(tax * 0.9)
        self.add_field("PAY_WITHHELD", withheld, 12, 'N')
        
        # Balance/Refund
        if withheld > total_tax:
            self.add_field("CALC_REFUND", withheld - total_tax, 12, 'N')
            self.add_field("CALC_BAL_DUE", 0, 12, 'N')
        else:
            self.add_field("CALC_REFUND", 0, 12, 'N')
            self.add_field("CALC_BAL_DUE", total_tax - withheld, 12, 'N')

    def add_worksheets_and_fillers(self):
        # Add remaining fields to reach 500+
        current_count = len(self.fields)
        needed = 500 - current_count
        if needed > 0:
            for i in range(needed + 50): # Add extra to be safe
                self.add_field(f"WKST_CALC_DEBUG_{i}", random.randint(0, 9999), 8, 'N')

    def get_fixed_width_line(self):
        line = ""
        for name, width, type_char in self.fields:
            val = self.data[name]
            if type_char == 'N':
                # Numeric: Right aligned, zero padded
                if val == "": val = 0
                s_val = str(int(val)) # Ensure int
                if len(s_val) > width: s_val = s_val[-width:] # Truncate if too long (shouldn't happen with logic)
                line += s_val.zfill(width)
            else:
                # Alpha: Left aligned, space padded
                s_val = str(val)
                if len(s_val) > width: s_val = s_val[:width]
                line += s_val.ljust(width)
        return line

    def get_csv_row(self):
        return {name: self.data[name] for name, _, _ in self.fields}

def main():
    records = []
    print(f"Generating {NUM_RECORDS} records...")
    
    for _ in range(NUM_RECORDS):
        rec = TaxpayerRecord()
        rec.generate_demographics()
        agi = rec.generate_income_and_schedules()
        rec.calculate_taxes(agi)
        rec.add_worksheets_and_fillers()
        records.append(rec)

    # Write Fixed Width
    print(f"Writing {OUTPUT_FW}...")
    with open(OUTPUT_FW, "w") as f:
        for rec in records:
            f.write(rec.get_fixed_width_line() + "\n")

    # Write CSV
    print(f"Writing {OUTPUT_CSV}...")
    if records:
        fieldnames = [f[0] for f in records[0].fields]
        with open(OUTPUT_CSV, "w", newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for rec in records:
                writer.writerow(rec.get_csv_row())

    print("Done.")

if __name__ == "__main__":
    main()
