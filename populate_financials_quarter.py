
import sqlite3, config
from sqlite3 import Error
import pandas as pd
import simfin as sf
import os
from simfin.names import *
import numpy as np
from datetime import date

connection = sqlite3.connect(config.DB_FILE)
connection.row_factory = sqlite3.Row
cursor = connection.cursor()
cursor.execute("""SELECT id, symbol, name FROM stock""")
rows = cursor.fetchall()

symbols = []
stock_dict = {}
for row in rows:
    symbol = row['symbol']
    symbols.append(symbol)
    stock_dict[symbol] = row['id']

#SIMFIN credentials and root drive
sf.set_data_dir(config.SIMFIN_DIR)
sf.set_api_key(api_key= config.API_KEY_SIMFIN)

#Get list of simFin Tickers & match with Alpaca Tickers
simfin_tickers = sf.load_companies(market='us').index
simfin_companies_name = sf.load_companies(market='us')['Company Name']
simfin_tickers = [simfin_tickers[i] for i in range(len(simfin_tickers))]

get_errors = []
ignore_list =[]
for ticks in simfin_tickers:
    try:
        stock_dict[ticks]
    except Exception as e:
        get_errors.append(e.args)

# Ignore list has simfin tickers which couldn't be matched/found with Alpaca
ignore_list = [list(get_errors[i])[0] for i in range(len(get_errors))]
final_ticker_list = [x for x in simfin_tickers if x not in ignore_list]

#Simfin Quarter Balance Statement
df_BS_q = sf.load(dataset='balance', variant='quarterly-full', market='us',
              index=[TICKER, FISCAL_YEAR, FISCAL_PERIOD],
              parse_dates=[REPORT_DATE, PUBLISH_DATE, RESTATED_DATE])

#Simfin Quarter Income Statement
df_IS_q = sf.load(dataset='income', variant='Quarterly-full', market='us',
              index=[TICKER, FISCAL_YEAR, FISCAL_PERIOD],
              parse_dates=[REPORT_DATE, PUBLISH_DATE, RESTATED_DATE])

#Simfin Quarter cashflow Statement
df_CF_q = sf.load(dataset='cashflow', variant='Quarterly-full', market='us',
              index=[TICKER, FISCAL_YEAR, FISCAL_PERIOD],
              parse_dates=[REPORT_DATE, PUBLISH_DATE, RESTATED_DATE])

#Some more companies giving keyerror when I index so removing them as well
get_errors1 = []
for ticks in final_ticker_list:
    try:
        df_BS_q.loc[ticks]
    except Exception as e:
        get_errors1.append(e.args)
ignore_list1 = [get_errors1[i][0] for i in range(len(get_errors1))]
final_ticker_list = [x for x in final_ticker_list if x not in ignore_list1]

#get industry id's - removed banks, insurance-life and insurance,  -create seperate.
industry_df = sf.load_companies(market='us').loc[final_ticker_list]
industry_df = industry_df[(industry_df['IndustryId'] != 104002) &
              (industry_df['IndustryId'] != 104004) &
              (industry_df['IndustryId'] != 104005) &
              (industry_df['IndustryId'] != 104006) &
              (industry_df['IndustryId'] != 104013)]

final_ticker_list = None
final_ticker_list = industry_df.index.values

#Get column names of Table "balance_Quarter_general" - we are using these to index our dataframe
connection = sqlite3.connect(config.DB_FILE)
cursor = connection.execute('select * from balance_quarter_general')
col_name_balance = [description[0] for description in cursor.description]
col_name_balance = [elem.upper() for elem in col_name_balance ]

#Get column names of Table "income_quarter_general" - we are using these to index our dataframe
connection = sqlite3.connect(config.DB_FILE)
cursor = connection.execute('select * from income_quarter_general')
col_name_income = [description[0] for description in cursor.description]
col_name_income = [elem.upper() for elem in col_name_income]

#Get column names of Table "cashflow_quarter_general" - we are using these to index our dataframe
connection = sqlite3.connect(config.DB_FILE)
cursor = connection.execute('select * from cashflow_quarter_general')
col_name_cashflow = [description[0] for description in cursor.description]
col_name_cashflow = [elem.upper() for elem in col_name_cashflow ]

# Since not pulling the first 4 columns of the table "balance_quarter_general":
col_name_balance = col_name_balance[4:]
col_len_balance = range(len(col_name_balance))

# Since not pulling the first 4 columns of the table "income_quarter_general":
col_name_income = col_name_income[4:]
col_len_income = range(len(col_name_income))

# Since not pulling the first 4 columns of the table "income_quarter_general":
col_name_cashflow = col_name_cashflow[4:]
col_len_cashflow = range(len(col_name_cashflow))
qtr_line_item = []


for ticks in final_ticker_list:
    one_company_df_balance = df_BS_q.loc[ticks]
    one_company_df_income = df_IS_q.loc[ticks]
    one_company_df_cashflow = df_CF_q.loc[ticks]

    # Get Years & quarters
    fiscal_year_list = [i for i in df_BS_q.loc[ticks].index]
    calendar_range = range(len(fiscal_year_list))
    years = []
    qtr = []
    for i in calendar_range:
        years.append(fiscal_year_list[i][0])
        qtr.append(fiscal_year_list[i][1])
    years = np.unique(years)
    qtr = np.unique(qtr)

    for yr in years:
        for qt in qtr:

            qtr_line_item = []
            for line_items in col_len_balance:

                try:
                    if (col_name_balance[line_items] == 'DISCONTINUED_OPERATIONS_ST'):
                        qtr_line_item.append(one_company_df_balance['Discontinued Operations (Short Term)'][yr][qt])
                    elif (col_name_balance[line_items] == 'DISCONTINUED_OPERATIONS_LT'):
                        qtr_line_item.append(one_company_df_balance['Discontinued Operations (Long Term)'][yr][qt])
                    else:
                        qtr_line_item.append(one_company_df_balance[pd.eval(col_name_balance[line_items], engine='python')][yr][qt])
                except Exception as e:
                    pass
            try:
                cursor.execute("""INSERT INTO balance_quarter_general (stock_id, fiscal_year, fiscal_period, report_date,  publish_date, restated_date, shares_basic, shares_diluted, cash_equiv_st_invest, cash_equiv, st_invest, acc_notes_recv, acc_recv_net, notes_recv_net, unbilled_revenue, inventories, raw_materials, work_in_process, finished_goods, other_inventory, other_st_assets, prepaid_expenses, deriv_hedg_assets_st, assets_hfs, def_tax_assets_st, income_tax_recv, discontinued_operations_st, misc_st_assets, total_cur_assets, ppe_net, ppe, accum_depr, lt_invest_recv, lt_invest, lt_market_sec, lt_recv, other_lt_assets, intangibles, goodwill, other_intangibles, prepaid_expense, def_tax_assets_lt, deriv_hedg_assets_lt, prepaid_pension_costs, discontinued_operations_lt, invest_affil, misc_lt_assets, total_noncur_assets, total_assets, payables_accruals, accounts_payable, accrued_taxes, int_div_payable, other_payables, st_debt, st_borrowings, st_capital_leases, cur_port_lt_debt, other_st_liab, def_revenue_st, liab_deriv_hedg_st, def_tax_liab_st, liab_discop_st, misc_st_liab, total_cur_liab, lt_debt, lt_borrowings, lt_capital_leases, other_lt_liab, accrued_liab, pension_liab, pensions, other_post_ret_ben, def_comp, def_revenue_lt, def_tax_liab_lt, liab_deriv_hedg_lt, liab_discop_lt, misc_lt_liab, total_noncur_liab, total_liab, preferred_equity, share_capital_add, common_stock, add_paid_in_capital, other_share_capital, treasury_stock, retained_earnings, other_equity, equity_before_minority, minority_interest, total_equity, total_liab_equity)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (stock_dict[ticks], int(yr), str(qt), qtr_line_item[0].date(), qtr_line_item[1].date(), qtr_line_item[2].date(), float(qtr_line_item[3]), float(qtr_line_item[4]), float(qtr_line_item[5]), float(qtr_line_item[6]), float(qtr_line_item[7]), float(qtr_line_item[8]), float(qtr_line_item[9]), float(qtr_line_item[10]), float(qtr_line_item[11]), float(qtr_line_item[12]), float(qtr_line_item[13]), float(qtr_line_item[14]), float(qtr_line_item[15]), float(qtr_line_item[16]), float(qtr_line_item[17]), float(qtr_line_item[18]), float(qtr_line_item[19]), float(qtr_line_item[20]), float(qtr_line_item[21]), float(qtr_line_item[22]), float(qtr_line_item[23]), float(qtr_line_item[24]), float(qtr_line_item[25]), float(qtr_line_item[26]), float(qtr_line_item[27]), float(qtr_line_item[28]), float(qtr_line_item[29]), float(qtr_line_item[30]), float(qtr_line_item[31]), float(qtr_line_item[32]), float(qtr_line_item[33]), float(qtr_line_item[34]), float(qtr_line_item[35]), float(qtr_line_item[36]), float(qtr_line_item[37]), float(qtr_line_item[38]), float(qtr_line_item[39]), float(qtr_line_item[40]), float(qtr_line_item[41]), float(qtr_line_item[42]), float(qtr_line_item[43]), float(qtr_line_item[44]), float(qtr_line_item[45]), float(qtr_line_item[46]), float(qtr_line_item[47]), float(qtr_line_item[48]), float(qtr_line_item[49]), float(qtr_line_item[50]), float(qtr_line_item[51]), float(qtr_line_item[52]), float(qtr_line_item[53]), float(qtr_line_item[54]), float(qtr_line_item[55]), float(qtr_line_item[56]), float(qtr_line_item[57]), float(qtr_line_item[58]), float(qtr_line_item[59]), float(qtr_line_item[60]), float(qtr_line_item[61]), float(qtr_line_item[62]), float(qtr_line_item[63]), float(qtr_line_item[64]), float(qtr_line_item[65]), float(qtr_line_item[66]), float(qtr_line_item[67]), float(qtr_line_item[68]), float(qtr_line_item[69]), float(qtr_line_item[70]), float(qtr_line_item[71]), float(qtr_line_item[72]), float(qtr_line_item[73]), float(qtr_line_item[74]), float(qtr_line_item[75]), float(qtr_line_item[76]), float(qtr_line_item[77]), float(qtr_line_item[78]), float(qtr_line_item[79]), float(qtr_line_item[80]), float(qtr_line_item[81]), float(qtr_line_item[82]), float(qtr_line_item[83]), float(qtr_line_item[84]), float(qtr_line_item[85]), float(qtr_line_item[86]), float(qtr_line_item[87]), float(qtr_line_item[88]), float(qtr_line_item[89])))
            except Exception as e:
                pass

            qtr_line_item = []
            for line_items in col_len_income:
                try:
                    if (col_name_income[line_items] == 'DISCONTINUED_OPERATIONS'):
                        qtr_line_item.append(one_company_df_income['Discontinued Operations'][yr][qt])
                    else:
                        qtr_line_item.append(one_company_df_income[pd.eval(col_name_income[line_items], engine='python')][yr][qt])
                except Exception as e:
                    pass
            try:
                cursor.execute("""INSERT INTO income_quarter_general (stock_id, fiscal_year, fiscal_period, report_date, publish_date, restated_date, shares_basic, shares_diluted, source, revenue, sales_services_revenue, financing_revenue, other_revenue, cost_revenue, cost_goods_services, cost_financing_revenue, cost_other_revenue, gross_profit, other_op_income, operating_expenses, selling_gen_admin, selling_marketing, gen_admin, research_dev, depr_amor, prov_doubt_acc, other_op_expense, op_income, non_op_income, interest_exp_net, interest_exp, interest_income, other_invest_income, fx_gain_loss, income_affil, other_non_op_income, pretax_income_loss_adj, abnorm_gain_loss, acq_in_process_rd, merger_acq_expense, abnorm_deriv, disposal_assets, early_ext_debt, asset_writedown, impair_goodwill_int, sale_business, legal_settlement, restr_charges, sale_investments, insurance_settlement, other_abnorm, pretax_income_loss, income_tax, tax_current, tax_deferred, tax_allowance, income_affil_net_tax, income_cont_op, net_extr_gain_loss, discontinued_operations, acc_charges, income_incl_minority, minority_interest, net_income, preferred_dividends, other_adj, net_income_common)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (stock_dict[ticks], int(yr), str(qt), qtr_line_item[0].date(), qtr_line_item[1].date(), qtr_line_item[2].date(), float(qtr_line_item[3]), float(qtr_line_item[4]), qtr_line_item[5], qtr_line_item[6], float(qtr_line_item[7]), float(qtr_line_item[8]), float(qtr_line_item[9]), float(qtr_line_item[10]), float(qtr_line_item[11]), float(qtr_line_item[12]), float(qtr_line_item[13]), float(qtr_line_item[14]), float(qtr_line_item[15]), float(qtr_line_item[16]), float(qtr_line_item[17]), float(qtr_line_item[18]), float(qtr_line_item[19]), float(qtr_line_item[20]), float(qtr_line_item[21]), float(qtr_line_item[22]), float(qtr_line_item[23]), float(qtr_line_item[24]), float(qtr_line_item[25]), float(qtr_line_item[26]), float(qtr_line_item[27]), float(qtr_line_item[28]), float(qtr_line_item[29]), float(qtr_line_item[30]), float(qtr_line_item[31]), float(qtr_line_item[32]), float(qtr_line_item[33]), float(qtr_line_item[34]), float(qtr_line_item[35]), float(qtr_line_item[36]), float(qtr_line_item[37]), float(qtr_line_item[38]), float(qtr_line_item[39]), float(qtr_line_item[40]), float(qtr_line_item[41]), float(qtr_line_item[42]), float(qtr_line_item[43]), float(qtr_line_item[44]), float(qtr_line_item[45]), float(qtr_line_item[46]), float(qtr_line_item[47]), float(qtr_line_item[48]), float(qtr_line_item[49]), float(qtr_line_item[50]), float(qtr_line_item[51]), float(qtr_line_item[52]), float(qtr_line_item[53]), float(qtr_line_item[54]), float(qtr_line_item[55]), float(qtr_line_item[56]), float(qtr_line_item[57]), float(qtr_line_item[58]), float(qtr_line_item[59]), float(qtr_line_item[60]), float(qtr_line_item[61]), float(qtr_line_item[62]), float(qtr_line_item[63])))
            except Exception as e:
                pass

            qtr_line_item = []
            for line_items in col_len_cashflow:
                try:
                    qtr_line_item.append(one_company_df_cashflow[pd.eval(col_name_cashflow[line_items], engine='python')][yr][qt])
                except Exception as e:
                    pass

            try:
                cursor.execute("""INSERT INTO cashflow_quarter_general (stock_id, fiscal_year, fiscal_period, report_date, publish_date, restated_date, shares_basic, shares_diluted, net_income_start, net_income, net_income_discop, other_adj, depr_amor, non_cash_items, stock_comp, def_income_taxes, other_non_cash_adj, chg_working_capital, chg_accounts_recv, chg_inventories, chg_acc_payable, chg_other, net_cash_discop_oper, net_cash_ops, capex, disp_fix_assets_int, disp_fix_assets, disp_int_assets, acq_fix_assets_int, purch_fix_assets, acq_int_assets, other_chg_fix_assets_int, net_chg_lt_invest, decr_lt_invest, incr_lt_invest, net_cash_acq_divest, net_cash_divest, cash_acq_sub, cash_joint_ventures, net_cash_other_acq, other_invest_act, net_cash_discop_invest, net_cash_inv, dividends_paid, cash_repay_debt, cash_repay_st_debt, cash_repay_lt_debt, repay_lt_debt, cash_lt_debt, cash_repurchase_equity, incr_capital_stock, decr_capital_stock, other_fin_act, net_cash_discop_fin, net_cash_fin, net_cash_before_discop_fx, chg_cash_discop_other, net_cash_before_fx, effect_fx_rates, net_chg_cash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                 (stock_dict[ticks], int(yr), str(qt), qtr_line_item[0].date(), qtr_line_item[1].date(), qtr_line_item[2].date(), float(qtr_line_item[3]), float(qtr_line_item[4]), float(qtr_line_item[5]), float(qtr_line_item[6]), float(qtr_line_item[7]), float(qtr_line_item[8]), float(qtr_line_item[9]), float(qtr_line_item[10]), float(qtr_line_item[11]), float(qtr_line_item[12]), float(qtr_line_item[13]), float(qtr_line_item[14]), float(qtr_line_item[15]), float(qtr_line_item[16]), float(qtr_line_item[17]), float(qtr_line_item[18]), float(qtr_line_item[19]), float(qtr_line_item[20]), float(qtr_line_item[21]), float(qtr_line_item[22]), float(qtr_line_item[23]), float(qtr_line_item[24]), float(qtr_line_item[25]), float(qtr_line_item[26]), float(qtr_line_item[27]), float(qtr_line_item[28]), float(qtr_line_item[29]), float(qtr_line_item[30]), float(qtr_line_item[31]), float(qtr_line_item[32]), float(qtr_line_item[33]), float(qtr_line_item[34]), float(qtr_line_item[35]), float(qtr_line_item[36]), float(qtr_line_item[37]), float(qtr_line_item[38]), float(qtr_line_item[39]), float(qtr_line_item[40]), float(qtr_line_item[41]), float(qtr_line_item[42]), float(qtr_line_item[43]), float(qtr_line_item[44]), float(qtr_line_item[45]), float(qtr_line_item[46]), float(qtr_line_item[47]), float(qtr_line_item[48]), float(qtr_line_item[49]), float(qtr_line_item[50]), float(qtr_line_item[51]), float(qtr_line_item[52]), float(qtr_line_item[53]), float(qtr_line_item[54]), float(qtr_line_item[55]), float(qtr_line_item[56])))
            except Exception as e:
                pass

    print(f"Inserted data for: {ticks}")

connection.commit()

print(f"Companies Inserted: {len(final_ticker_list)}")
