import requests

def fetch_bitcoin_price():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
    
    try:
        response = requests.get(url)
        response.raise_for_status()  
        
        data = response.json()
        bitcoin_price = data['bitcoin']['usd']
        return bitcoin_price
    
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error occurred: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout error occurred: {timeout_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"An error occurred: {req_err}")
    except KeyError:
        print("Error: Unexpected response format.")
    
    return None

if __name__ == "__main__":
    price = fetch_bitcoin_price()
    if price is not None:
        print(f"The current price of Bitcoin is: ${price}")
    else:
        print("Failed to fetch the Bitcoin price.")