from pathlib import Path
from typing import Annotated

import typer

pkg_app = typer.Typer()


@pkg_app.command(name="create")
def create_package(
    name: Annotated[str, typer.Argument(help="Name of the package to create")],
    user_dir: Annotated[
        str, typer.Option("--user-dir", "-u", help="The directory containing EOS user configurations")
    ] = "./user",
) -> None:
    """Create a new package with the specified name in the user directory."""
    package_dir = Path(user_dir) / name
    subdirs = ["devices", "tasks", "labs", "experiments"]

    try:
        package_dir.mkdir(parents=True, exist_ok=False)
        for subdir in subdirs:
            (package_dir / subdir).mkdir()

        # Create README.md with just the package name
        readme_content = f"# {name}"
        readme_path = package_dir / "README.md"
        readme_path.write_text(readme_content)

        typer.echo(f"Successfully created package '{name}' in {package_dir}")
    except FileExistsError:
        typer.echo(f"Error: Package '{name}' already exists in {user_dir}", err=True)
    except Exception as e:
        typer.echo(f"Error creating package: {e!s}", err=True)
