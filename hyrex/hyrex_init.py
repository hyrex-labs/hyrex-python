"""Interactive init script for setting up a Hyrex project."""

import os
import sys
from pathlib import Path

try:
    from colorama import init as colorama_init, Fore, Style
    colorama_init(autoreset=True)
    HAS_COLORAMA = True
except ImportError:
    HAS_COLORAMA = False
    # Simple fallback
    class Fore:
        GREEN = YELLOW = RED = CYAN = ""
    class Style:
        RESET_ALL = ""


def print_banner():
    """Print welcome banner."""
    print(f"\n{Fore.CYAN}{'=' * 50}")
    print("Hyrex Project Init")
    print(f"{'=' * 50}{Style.RESET_ALL}\n")


def get_project_name() -> str:
    """Get project name from user."""
    while True:
        name = input(f"{Fore.GREEN}Project name:{Style.RESET_ALL} ").strip()
        if not name:
            print(f"{Fore.RED}Project name is required.{Style.RESET_ALL}")
            continue
        
        # Basic validation - alphanumeric, hyphens, underscores
        if name.replace("-", "").replace("_", "").isalnum():
            return name
        else:
            print(f"{Fore.RED}Invalid name. Use only letters, numbers, hyphens, and underscores.{Style.RESET_ALL}")


def get_project_directory(default_name: str) -> Path:
    """Get project directory from user."""
    current_dir = Path.cwd()
    
    # Show current directory as default
    dir_input = input(f"{Fore.GREEN}Directory [.]:{Style.RESET_ALL} ").strip()
    
    if not dir_input or dir_input == ".":
        return current_dir
    else:
        return Path(dir_input).expanduser().resolve()


def get_connection_type() -> str:
    """Ask user for connection type."""
    print(f"\n{Fore.CYAN}Connection Type:{Style.RESET_ALL}")
    print("1. PostgreSQL Database (self-hosted)")
    print("2. Hyrex Cloud")
    
    while True:
        choice = input(f"\n{Fore.GREEN}Select option (1 or 2):{Style.RESET_ALL} ").strip()
        if choice == "1":
            return "postgres"
        elif choice == "2":
            return "cloud"
        else:
            print(f"{Fore.RED}Please enter 1 or 2.{Style.RESET_ALL}")


def get_database_url() -> str:
    """Get PostgreSQL database URL."""
    print(f"\n{Fore.YELLOW}Example: postgresql://user:password@localhost:5432/dbname{Style.RESET_ALL}")
    
    while True:
        url = input(f"{Fore.GREEN}Database URL:{Style.RESET_ALL} ").strip()
        if url:
            return url
        else:
            print(f"{Fore.RED}Database URL is required.{Style.RESET_ALL}")


def get_api_key() -> str:
    """Get Hyrex Cloud API key."""
    print(f"\n{Fore.YELLOW}Get your API key from: https://app.hyrex.io/settings{Style.RESET_ALL}")
    
    while True:
        key = input(f"{Fore.GREEN}API Key:{Style.RESET_ALL} ").strip()
        if key:
            return key
        else:
            print(f"{Fore.RED}API key is required.{Style.RESET_ALL}")


def should_overwrite_file(file_path: Path) -> bool:
    """Ask user if they want to overwrite an existing file."""
    if not file_path.exists():
        return True
    
    response = input(f"{Fore.YELLOW}File {file_path.name} already exists. Overwrite? (y/N):{Style.RESET_ALL} ").strip().lower()
    return response == 'y'


def create_env_file(directory: Path, connection_type: str, credential: str):
    """Create .env file with credentials."""
    env_path = directory / ".env"
    
    if not should_overwrite_file(env_path):
        print(f"{Fore.YELLOW}Skipped{Style.RESET_ALL} .env file")
        return
    
    env_content = "# Hyrex Configuration\n"
    
    if connection_type == "postgres":
        env_content += f"HYREX_DATABASE_URL={credential}\n"
    else:  # cloud
        env_content += f"HYREX_API_KEY={credential}\n"
    
    env_path.write_text(env_content)
    print(f"{Fore.GREEN}✓{Style.RESET_ALL} Created .env file")


def create_hyrex_app_file(directory: Path, project_name: str):
    """Create hyrex_app.py file."""
    app_path = directory / "hyrex_app.py"
    
    if not should_overwrite_file(app_path):
        print(f"{Fore.YELLOW}Skipped{Style.RESET_ALL} hyrex_app.py")
        return
    
    app_content = f'''from hyrex import HyrexApp

from tasks import hy as registry

app = HyrexApp("{project_name}")

app.add_registry(registry)
'''
    
    app_path.write_text(app_content)
    print(f"{Fore.GREEN}✓{Style.RESET_ALL} Created hyrex_app.py")


def create_tasks_file(directory: Path):
    """Create tasks.py file with sample task."""
    tasks_path = directory / "tasks.py"
    
    if not should_overwrite_file(tasks_path):
        print(f"{Fore.YELLOW}Skipped{Style.RESET_ALL} tasks.py")
        return
    
    tasks_content = '''import random
import time

from hyrex import HyrexRegistry

hy = HyrexRegistry()


@hy.task
def test_task():
    """A simple test task that sleeps for a random duration."""
    sleep_duration = random.uniform(0, 2)
    time.sleep(sleep_duration)
'''
    
    tasks_path.write_text(tasks_content)
    print(f"{Fore.GREEN}✓{Style.RESET_ALL} Created tasks.py")


def create_requirements_file(directory: Path):
    """Create minimal requirements.txt file."""
    requirements_path = directory / "requirements.txt"
    
    if not should_overwrite_file(requirements_path):
        print(f"{Fore.YELLOW}Skipped{Style.RESET_ALL} requirements.txt")
        return
    
    requirements_content = '''hyrex
python-dotenv
'''
    
    requirements_path.write_text(requirements_content)
    print(f"{Fore.GREEN}✓{Style.RESET_ALL} Created requirements.txt")


def create_dockerfile(directory: Path):
    """Create Dockerfile."""
    dockerfile_path = directory / "Dockerfile"
    
    if not should_overwrite_file(dockerfile_path):
        print(f"{Fore.YELLOW}Skipped{Style.RESET_ALL} Dockerfile")
        return
    
    dockerfile_content = '''FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["hyrex", "run-worker", "hyrex_app:app"]
'''
    
    dockerfile_path.write_text(dockerfile_content)
    print(f"{Fore.GREEN}✓{Style.RESET_ALL} Created Dockerfile")


def main():
    """Main entry point for hyrex init."""
    try:
        print_banner()
        
        # Get project details
        project_name = get_project_name()
        project_dir = get_project_directory(project_name)
        
        # Get connection details
        connection_type = get_connection_type()
        
        if connection_type == "postgres":
            credential = get_database_url()
        else:
            credential = get_api_key()
        
        # Create project directory
        project_dir.mkdir(parents=True, exist_ok=True)
        
        # Calculate relative path for display
        try:
            relative_dir = project_dir.relative_to(Path.cwd())
        except ValueError:
            # If project_dir is not relative to cwd, use the full path
            relative_dir = project_dir
        
        print(f"\n{Fore.GREEN}✓{Style.RESET_ALL} Created directory: {relative_dir}")
        
        # Create .env file
        create_env_file(project_dir, connection_type, credential)
        
        # Create hyrex_app.py file
        create_hyrex_app_file(project_dir, project_name)
        
        # Create tasks.py file
        create_tasks_file(project_dir)
        
        # Create requirements.txt file
        create_requirements_file(project_dir)
        
        # Create Dockerfile
        create_dockerfile(project_dir)
        
        print(f"\n{Fore.GREEN}Project '{project_name}' initialized successfully!{Style.RESET_ALL}")
        
        # Print next steps
        print(f"\n{Fore.CYAN}Next steps:{Style.RESET_ALL}")
        
        step = 1
        # Only show cd command if not current directory
        if relative_dir != Path("."):
            print(f"{step}. cd {relative_dir}")
            step += 1
        
        # Show worker command
        print(f"{step}. hyrex run-worker hyrex_app:app")
        
    except KeyboardInterrupt:
        print(f"\n\n{Fore.YELLOW}Setup cancelled.{Style.RESET_ALL}")
        sys.exit(0)
    except Exception as e:
        print(f"\n{Fore.RED}Error: {e}{Style.RESET_ALL}")
        sys.exit(1)


if __name__ == "__main__":
    main()