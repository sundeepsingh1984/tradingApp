import math

#Instead of hardcoding 1000, better would be to get the balance amount from alpaca and use that as numerator
def calculate_quantity(price):
    quantity = math.floor(10000/price)
    return quantity
