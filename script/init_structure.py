import os
import sys
from pathlib import Path

# 1Define the Oracle-aligned architecture tree for the NASA Harmonization & Quantticks pipeline
ORACLE_NASA_STRUCTURE = {
    "00-discovery-layer": {
        "files": {
            "discovery_0000.py": "# NASA FIRMS API discovery & bounding box polling\n",
            "SOP_0000_DISCOVERY.md": "# SOP: Discovery Layer Operations\n"
        }
    },
    "11-ingestion-layer": {
        "files": {
            "ingestion_1111.py": "# Raw CSV extraction & staging into D:/nasa\n"
        },
        "sublayers": {
            "110-harmonization-layer": {
                "files": {
                    "harmonize_modis_viirs.py": "# Spatial alignment, confidence mapping, & FRP calibration\n",
                    "SOP_110_HARMONIZATION.md": "# SOP: MODIS & VIIRS Multi-Sensor Harmonization\n"
                }
            },
            "111-storage-parquet-layer": {
                "files": {
                    "storage_parquet.py": "# Partitioned Parquet lake writer (ZSTD compressed)\n",
                    "SOP_111_STORAGE.md": "# SOP: Zero-Bloat Disk Persistence & Parquet Lake\n"
                }
            }
        }
    },
    "44-backtesting-layer": {
        "files": {
            "backtest_fire_regimes.py": "# Historical anomaly backtests against multi-year baselines\n",
            "SOP_44_BACKTESTING.md": "# SOP: Validation and Simulation Outcomes\n"
        }
    },
    "55-research-layer": {
        "files": {
            "continuous_improvement_research.py": "# Core continuous improvement layer: monitors metrics and triggers research outcomes\n",
            "SOP_55_RESEARCH.md": "# SOP: Adaptive Metric-Driven Research & Improvement\n"
        }
    },
    "77-chronicle-layer": {
        "files": {
            "execution_audit.log": "# System telemetry and operational execution logs\n"
        }
    },
    "99-librarian-layer": {
        "files": {
            "dataset_registry.json": "{\n  \"datasets\": [],\n  \"partitions\": {}\n}\n"
        }
    },
    "config": {
        "files": {
            "00_nasa_service_config.yaml": "# Pydantic-validated service configuration\n"
        }
    }
}

def create_oracle_structure(base_path: str = "."):
    root = Path(base_path)
    print(f"🏗️ Verifying and building Oracle Brain Layer structure at: {root.absolute()}")

    def _build_tree(current_dir: Path, structure: dict):
        for folder_name, content in structure.items():
            folder_path = current_dir / folder_name
            
            # 1. Check and create directory if it doesn't exist
            if not folder_path.exists():
                folder_path.mkdir(parents=True, exist_ok=True)
                print(f"📁 Created missing directory: {folder_path}")
            else:
                print(f"📂 Verified existing directory: {folder_path}")

            # 2. Check and create required files inside the directory
            files = content.get("files", {})
            for file_name, template_content in files.items():
                file_path = folder_path / file_name
                if not file_path.exists():
                    with open(file_path, "w", encoding="utf-8") as f:
                        f.write(template_content)
                    print(f"   📄 Created file: {file_path}")
                else:
                    print(f"   📄 File already exists (skipped): {file_path}")

            # 3. Recursively build sublayers if defined
            sublayers = content.get("sublayers", {})
            if sublayers:
                _build_tree(folder_path, sublayers)

    try:
        _build_tree(root, ORACLE_NASA_STRUCTURE)
        print("\n✅ Oracle architecture folder structure successfully verified and synchronized!")
    except Exception as e:
        print(f"❌ Error building directory structure: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Target current directory or parent workspace
    target_root = os.getenv("ORACLE_ROOT_DIR", ".")
    create_oracle_structure(target_root)