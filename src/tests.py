import pytest
import main
import pandas as pd
import datetime

class Test():
    d = {'CMTE_ID':['C00629618', 'C00629618', 'C00629618'], 'NAME':['PEREZ, JOHN A', 'PEREZ, JOHN A', 'PEREZ, JOHN A'], 'ZIP_CODE':['90017', '123', '90017'], 'TRANSACTION_DT':['01032017', '01032017', '0103201'], 'TRANSACTION_AMT':[40, 30, 20], 'OTHER_ID':['', '', '']}
    df = pd.DataFrame(data=d)
    recorded_donors = {'PEREZ, JOHN A 90017': 2016, 'James 01235': 2015, 'Emma 02140': 2013}

    def test_apply_filters_true(self):
        for idx, row in enumerate(self.df.itertuples()):
            if idx == 0:
                returned_zip_code, returned_date = main.apply_filters(row)
                expected_date = datetime.date(2017, 1, 3)
                assert returned_zip_code == '90017' \
                    and expected_date.year == returned_date.year \
                    and expected_date.month == returned_date.month \
                    and expected_date.day == returned_date.day
            if idx == 1:
                break

    def test_apply_filters_wrong_zip(self):
        for idx, row in enumerate(self.df.itertuples()):
            if idx == 0:
                continue
            if idx == 1:
                assert main.apply_filters(row) == (None, None)

    def test_apply_filters_wrong_date(self):
        for idx, row in enumerate(self.df.itertuples()):
            if idx == 0 or idx == 1:
                continue
            if idx == 2:
                assert main.apply_filters(row) == ('90017', None)

    def is_repeat_donor_same_year(self):
        date = datetime.date(2017, 1, 3)
        row = self.df.ix[0]
        assert main.is_repeat_donor(row, row.ZIP_CODE, date, self.recorded_donors) == False

    def is_repeat_donor_same_true(self):
        date = datetime.date(2017, 1, 3)
        row = self.df.ix[0]
        assert main.is_repeat_donor(row, row.ZIP_CODE, date, self.recorded_donors) == True

# obj = Test()
# obj.is_repeat_donor_same_true()




