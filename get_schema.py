
import json
from generate_imf_data import TaxpayerRecord

def get_schema():
    rec = TaxpayerRecord()
    rec.generate_demographics()
    # We need to pass a dummy AGI to generate_income_and_schedules if it needed it, 
    # but looking at the code, generate_income_and_schedules RETURNS agi, it doesn't take it.
    # Wait, let's check the code again.
    # Line 59: def generate_income_and_schedules(self): ... returns agi
    # Line 114: def calculate_taxes(self, agi): ... takes agi
    
    agi = rec.generate_income_and_schedules()
    rec.calculate_taxes(agi)
    rec.add_worksheets_and_fillers()
    
    # rec.fields is a list of (name, width, type_char)
    return rec.fields

if __name__ == "__main__":
    fields = get_schema()
    print(json.dumps(fields))
