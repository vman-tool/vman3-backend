# my_cli_tool/cli.py
import requests
import typer

app = typer.Typer()

@app.command()
def update_details(uuid: str, name: str, email: str):
    url = "http://127.0.0.1:8000/update-details"
    payload = {"uuid": uuid, "name": name, "email": email}
    response = requests.put(url, json=payload)
    if response.status_code == 200:
        print(response.json())
    else:
        print("Failed to update:", response.json())

def main():
    app()

if __name__ == "__main__":
    main()
