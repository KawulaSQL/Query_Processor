# main.py
from Client import Client

if __name__ == "__main__":
    client = Client()

    address = input(">>> Connect to KawulaSQL: ")
    host, port = address.split(':')

    try:
        client.connect(host, int(port))
        print("Connected to address " + address)
        print("Welcome to KawulaSQL!")
        print("Enter your SQL query or type 'exit' to quit.\n")

        while True:
            query = input("Please enter your SQL query: ").strip()
            dummy = "SELECT * FROM department;"

            if query.lower() == "exit":
                print("Exiting KawulaSQL. Goodbye!")
                break
            try:
                client.send(query.encode())
                client.send(dummy.encode())
            except Exception as e:
                print(f"Error while sending to server: {str(e)}")

            try:
                response = client.receive()

                print(response)

            except Exception as e:
                print(f"Server Response Error: {str(e)}")


    except Exception as e:
        print(f">>> Connection Error: {str(e)}")
