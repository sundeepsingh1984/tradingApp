import sqlite3, config
import alpaca_trade_api as tradeapi
from fastapi import FastAPI, Request, Form
from fastapi.templating import Jinja2Templates
from fastapi.responses import RedirectResponse
from datetime import date
import datetime as dt
import numpy as np



app = FastAPI()
templates = Jinja2Templates(directory="templates")

@app.get("/")
def index(request: Request):

    stock_filter = request.query_params.get('filter', False)

    connection = sqlite3.connect(config.DB_FILE)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()

    if stock_filter == 'new_closing_highs':
    # where date equals the maximum date available in database
        cursor.execute("""
                SELECT * FROM  (SELECT symbol, name,  stock_id, max(close), date 
                FROM stock_price JOIN stock on stock.id = stock_price.stock_id 
                GROUP BY stock_id 
                ORDER BY  symbol) WHERE date = (select max(date) from stock_price) """)

    elif stock_filter == 'new_closing_lows':
        cursor.execute("""
                        SELECT * FROM  (SELECT symbol, name,  stock_id, min(close), date 
                        FROM stock_price JOIN stock on stock.id = stock_price.stock_id 
                        GROUP BY stock_id 
                        ORDER BY  symbol) WHERE date = (select max(date) from stock_price) """)

    elif stock_filter == 'rsi_overbought':
        cursor.execute("""
                       SELECT symbol, name,  stock_id, date 
                       FROM stock_price JOIN stock on stock.id = stock_price.stock_id 
                       WHERE rsi_14 > 90 
                       AND date = (select max(date) from stock_price)
                       ORDER BY  symbol
                        """)

    elif stock_filter == 'rsi_oversold':
        cursor.execute("""
                               SELECT symbol, name,  stock_id, date 
                               FROM stock_price JOIN stock on stock.id = stock_price.stock_id 
                               WHERE rsi_14 < 20 
                               AND date = (select max(date) from stock_price)
                               ORDER BY  symbol
                                """)
    elif stock_filter == 'above_sma_20':
        cursor.execute("""
                               SELECT symbol, name,  stock_id, date 
                               FROM stock_price JOIN stock on stock.id = stock_price.stock_id 
                               WHERE close > sma_20 
                               AND date = (select max(date) from stock_price)
                               ORDER BY  symbol
                                """)

    elif stock_filter == 'below_sma_20':
        cursor.execute("""
                               SELECT symbol, name,  stock_id, date 
                               FROM stock_price JOIN stock on stock.id = stock_price.stock_id 
                               WHERE close < sma_20 
                               AND date = (select max(date) from stock_price)
                               ORDER BY  symbol
                                """)

    elif stock_filter == 'above_sma_50':
        cursor.execute("""
                               SELECT symbol, name,  stock_id, date 
                               FROM stock_price JOIN stock on stock.id = stock_price.stock_id 
                               WHERE close > sma_50 
                               AND date = (select max(date) from stock_price)
                               ORDER BY  symbol
                                """)

    elif stock_filter == 'below_sma_50':
        cursor.execute("""
                               SELECT symbol, name,  stock_id, date 
                               FROM stock_price JOIN stock on stock.id = stock_price.stock_id 
                               WHERE close < sma_50 
                               AND date = (select max(date) from stock_price)
                               ORDER BY  symbol
                                """)

    else:
        cursor.execute("""
                SELECT id, symbol, name FROM stock ORDER BY symbol
        """)

    rows = cursor.fetchall()

    current_date = date.today().isoformat()

    cursor.execute("""
    SELECT symbol , rsi_14, sma_20, sma_50, close
    from stock join stock_price on stock_price.stock_id = stock.id
    where date = (select max(date) from stock_price);    
    """)

    indicator_rows = cursor.fetchall()
    indicator_values = {}

    for row in indicator_rows:
        indicator_values[row['symbol']] = row

    return templates.TemplateResponse("index.html", {"request": request, "stocks": rows, 'indicator_values': indicator_values})


@app.get("/stock/{symbol}")
def stock_detail(request: Request, symbol):

    connection = sqlite3.connect(config.DB_FILE)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()

    cursor.execute("""
            SELECT * FROM strategy
        """)
    strategies = cursor.fetchall()

    cursor.execute("""
            SELECT id, symbol, name FROM stock WHERE symbol = ?
        """,(symbol,))
    row = cursor.fetchone()

    cursor.execute("""
            SELECT * FROM stock_price WHERE stock_id = ? ORDER BY date DESC
        """,(row['id'],))

    prices = cursor.fetchall()

    return templates.TemplateResponse("stock_detail.html", {"request": request, "stock": row, "bars":prices, "strategies": strategies})


#FUNDAMENTALS START

@app.get("/fundamentals/{stock_id}")
def simfin_stock_details(request: Request,stock_id ):


    frequency_filter = request.query_params.get('filter_f', False)


    connection = sqlite3.connect(config.DB_FILE)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()

    #Number of years to display
    years_to_show = 8
    cursor.execute("""
                SELECT stock_id, fiscal_year FROM income_annual_general
            WHERE stock_id = ? ORDER BY fiscal_year DESC
        """,(stock_id,))

    fiscal_year = cursor.fetchall()

    #Balance Sheet - Annual
    connection = sqlite3.connect(config.DB_FILE)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()
    cursor.execute("""
            SELECT *, name, symbol FROM balance_annual_general
            JOIN stock on balance_annual_general.stock_id =  stock.id
            WHERE stock_id = ? ORDER BY fiscal_year DESC
        """,(stock_id,))

    balance_sheet_rows = cursor.fetchall()
    balance_sheet_rows = balance_sheet_rows[:years_to_show]

    # Get column names of Table "Balance_annual_general" - we are using these to render the balance table
    connection = sqlite3.connect(config.DB_FILE)
    cursor = connection.execute('select * from balance_annual_general')
    col_name_balance = [description[0] for description in cursor.description]
    # Only get the column indices we want to print
    indices = [9, 12, 16, 21, 29, 30, 33, 37, 48, 49, 50, 55, 59, 65, 66, 69, 80, 81, 83, 87, 88, 89, 90, 91, 92, 93]
    selected_elements = [col_name_balance[index] for index in indices]
    indices = []

    # Get rid of any lineitems throwing error (i.e any empty values)
    j = 0
    selected_elements_final_balance = {}
    for i, elem in enumerate(selected_elements):
        if (balance_sheet_rows[0][elem] != None):
            selected_elements_final_balance[j] = elem
            j += 1
    selected_elements_final_balance = list(selected_elements_final_balance.values())
    selected_elements = []

    #cashflow - Annual
    connection = sqlite3.connect(config.DB_FILE)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()
    cursor.execute("""
            SELECT *, name, symbol FROM cashflow_annual_general
            JOIN stock on cashflow_annual_general.stock_id =  stock.id
            WHERE stock_id = ? ORDER BY fiscal_year DESC
        """,(stock_id,))

    cashflow_rows = cursor.fetchall()
    cashflow_rows = cashflow_rows[:years_to_show]

    # Get column names of Table "cashflow_annual_general" - we are using these to render the cashflow table
    connection = sqlite3.connect(config.DB_FILE)
    cursor = connection.execute('select * from cashflow_annual_general')
    col_name_cashflow = [description[0] for description in cursor.description]
    # Only get the column indices we want to print
    indices = [9, 13, 14, 18, 24, 25, 32, 41, 43, 44, 45, 50, 53, 55, 58, 59, 60]
    selected_elements = [col_name_cashflow[index] for index in indices]
    indices = []

    # Get rid of any lineitems throwing error (i.e any empty values)
    j = 0
    selected_elements_final_cashflow = {}
    for i, elem in enumerate(selected_elements):
        if (cashflow_rows[0][elem] != None):
            selected_elements_final_cashflow[j] = elem
            j += 1
    selected_elements_final_cashflow = list(selected_elements_final_cashflow.values())
    selected_elements = []


    # Income Statement - Annual
    connection = sqlite3.connect(config.DB_FILE)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()

    cursor.execute("""
            SELECT *, name, symbol FROM income_annual_general 
            JOIN stock on income_annual_general.stock_id =  stock.id
            WHERE stock_id = ? ORDER BY fiscal_year DESC
        """,(stock_id,))

    income_statement_rows = cursor.fetchall()
    income_statement_rows = income_statement_rows[:years_to_show]
    fiscal_year = fiscal_year[:years_to_show]

    # Get column names of Table "income_annual_general" - we are using these to render the income table
    connection = sqlite3.connect(config.DB_FILE)
    cursor = connection.execute('select * from income_annual_general')
    col_name_income = [description[0] for description in cursor.description]
    # Only get the column indices we want to print
    indices = [10,14,18,20,28,29,52,53,62,63,67,4,5,6]
    selected_elements = [col_name_income[index] for index in indices]

    # Get rid of any lineitems throwing error (i.e any empty values)
    j = 0
    selected_elements_final_income = {}
    for i, elem in enumerate(selected_elements):
        if (income_statement_rows[0][elem] != None):
            selected_elements_final_income[j] = elem
            j += 1
    selected_elements_final_income = list(selected_elements_final_income.values())


    return templates.TemplateResponse("simfin_stock_details.html", {"request": request, "fiscal_year": fiscal_year,
        "balance_sheet": balance_sheet_rows,"selected_elements_balance": selected_elements_final_balance,
        "cashflow": cashflow_rows,"selected_elements_cashflow": selected_elements_final_cashflow,
        "income_statement": income_statement_rows,"selected_elements_income": selected_elements_final_income})


# FUNDAMENTALS MAIN PAGE

@app.get("/fundamentals")
def simfin(request: Request):

    connection = sqlite3.connect(config.DB_FILE)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()

    cursor.execute("""
                SELECT DISTINCT stock_id,name, symbol FROM income_annual_general 
                JOIN stock on income_annual_general.stock_id =  stock.id
        """)

    fundamentals = cursor.fetchall()

    # Some stock aren't being displayed - throwing error check (AAMC, 6789 etc)

    return templates.TemplateResponse("simfin.html", {"request": request,  "fundamentals": fundamentals})


#FUNDAMENTALS END



#NEW CODE HERE

@app.post("/apply_strategy")
def apply_strategy(strategy_id: int = Form(...), stock_id: int = Form(...)):

    connection = sqlite3.connect(config.DB_FILE)
    cursor = connection.cursor()

    cursor.execute("""
            INSERT INTO stock_strategy(stock_id, strategy_id) VALUES(?,?)
        """,(stock_id,strategy_id))

    connection.commit()

    return RedirectResponse(url=f"/strategy/{strategy_id}",status_code=303)


@app.get("/strategies")
def strategies(request: Request):
    connection = sqlite3.connect(config.DB_FILE)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()

    cursor.execute("""
            SELECT * FROM strategy
        """)

    strategies = cursor.fetchall()
    return templates.TemplateResponse("strategies.html", {"request": request, 'strategies':strategies})

@app.get("/orders")
def orders(request: Request):

    api = tradeapi.REST(config.API_KEY, config.SECRET_KEY, base_url=config.API_URL)
    orders = api.list_orders(status='all')

    return templates.TemplateResponse("orders.html", {"request": request, "orders":orders})

@app.get("/strategy/{strategy_id}")
def strategy(request: Request, strategy_id):

    connection = sqlite3.connect(config.DB_FILE)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()

    cursor.execute("""
            SELECT * FROM strategy
        """)
    strategies = cursor.fetchall()

    cursor.execute("""
            SELECT id, name FROM strategy WHERE id = ?
        """,(strategy_id,))
    strategy = cursor.fetchone()

    cursor.execute("""
            SELECT symbol, name 
            FROM stock JOIN stock_strategy on stock_strategy.stock_id = stock.id 
            WHERE strategy_id = ?
        """,(strategy_id,))

    stocks = cursor.fetchall()

    return templates.TemplateResponse("strategy.html", {"request": request, "stocks": stocks, "strategy": strategy})






