"""Custom setuptools build hook for packaging dbt project assets.

This repository keeps the dbt project files at the repo root, while the built
wheel expects those assets to live under ``dbt_affiliate_api_bundle`` so the
installed package can resolve a self-contained project directory.

Package metadata is defined in ``pyproject.toml``. This file exists only to
customize the wheel build: it copies ``affiliate_config.yml``,
``dbt_project.yml``, ``profiles.yml``, and the ``models/`` and ``macros/``
directories into the wheel under ``dbt_affiliate_api_bundle``.
"""

from __future__ import annotations

from pathlib import Path
import shutil

from setuptools import setup
from setuptools.command.build_py import build_py as _build_py


class build_py(_build_py):
    """Stage dbt project assets into the wheel package at build time."""

    def run(self) -> None:
        package_root = Path(self.build_lib) / "dbt_affiliate_api_bundle"
        legacy_package_root = Path(self.build_lib) / "dbt_affiliate_api"
        if package_root.exists():
            shutil.rmtree(package_root)
        if legacy_package_root.exists():
            shutil.rmtree(legacy_package_root)

        super().run()

        project_root = Path(__file__).resolve().parent
        package_root.mkdir(parents=True, exist_ok=True)

        for filename in ("affiliate_config.yml", "dbt_project.yml", "profiles.yml"):
            shutil.copy2(project_root / filename, package_root / filename)

        target_models_dir = package_root / "models"
        target_models_dir.mkdir(parents=True, exist_ok=True)
        for pattern in ("*.py", "*.sql"):
            for model_path in (project_root / "models").glob(pattern):
                shutil.copy2(model_path, target_models_dir / model_path.name)

        target_macros_dir = package_root / "macros"
        target_macros_dir.mkdir(parents=True, exist_ok=True)
        for macro_path in (project_root / "macros").glob("*.sql"):
            shutil.copy2(macro_path, target_macros_dir / macro_path.name)


setup(cmdclass={"build_py": build_py})
