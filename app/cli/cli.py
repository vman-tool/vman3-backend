# my_cli_tool/cli.py
import asyncio
import getpass
from urllib.parse import urlencode
from fastapi import Depends
import requests
import typer
from app.users.models.user import User
from app.shared.configs.arangodb import get_arangodb_session
from app.users.schemas.user import RegisterUserRequest
from app.users.services.user import create_or_update_user_account

app = typer.Typer()
user_app = typer.Typer()
app.add_typer(user_app, name="update-password")

@user_app.callback(invoke_without_command=True)
def details(email: str = typer.Option(..., "--email", "-e", help="User's email")):
    """
    Update user details by email.
    """
    try:

        db = None
        async def get_session():
            async for session in get_arangodb_session():
                db = session
                return db
        db =  asyncio.run(get_session())

        if db is None:
            typer.echo("Failed to get database session")
            return
        
        count = 0
        while count < 3:
            password = getpass.getpass("Enter password: ")
            confirm_password = getpass.getpass("Confirm password: ")

            count += 1
            
            if password == confirm_password:
                break
            else:
                if count == 3:
                    typer.echo("You've tried 3 times and couldn't make it. Please start over.")
                    return
                else:
                    typer.echo("Passwords do not match. Please try again.")
        
        users = asyncio.run(User.get_many(filters={'email': email, "is_active": True, "created_by": None}, db=db))
        if len(users) > 1:
            typer.echo(f"Found multiple active users with email: {email}")
            return
        
        user = users[0]
        
        if not user:
            typer.echo(f"No active user found with email: {email}")
            return
        
        user = RegisterUserRequest(
            uuid=user['uuid'],
            name=user['name'],
            email=email,
            password=password,
            confirm_password=confirm_password
        )
        updated_user = asyncio.run(create_or_update_user_account(data=user, db=db))
        
        # Handle response
        if updated_user:
            typer.echo("Details updated successfully!")
            print({
                "name": updated_user["name"],
                "email": updated_user["email"],
                "is_active": updated_user["is_active"]
            })
        else:
            typer.echo("Failed to update user. Please try again")
            return
    except Exception as e:
        typer.echo(f"An error occurred: {str(e)}")

def main():
    # app()
    typer.run(details)

if __name__ == "__main__":
    main()
