import time
from Login import Login

if __name__ == "__main__":
    # Record the start time
    start_time = time.time()

    """-------------------------------------------------- Login to the API -----------------------------------------------"""

    # Create an instance of the LoginManager class and login to the API
    login_manager = Login()
    session_data = login_manager.login()
    print("\nSession Data: \n",session_data)
    

    if session_data['status'] == 'success':
        print("\nLogin successful!")
        print(f"\nLogin successful! in {time.time() - start_time:.2f} seconds")

        # Get the smart connect object from the session data
        smart_connect = session_data['connection']
        print("\nSmart Connect Object:\n", smart_connect)       
    else:
        print("\nLogin failed!")

    """------------------------------------------------- End of Login --------------------------------------------------"""