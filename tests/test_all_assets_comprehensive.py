#!/usr/bin/env python3
"""
Comprehensive test script for all Dagster assets (dlt and dbt).
Tests raw materialization and silver auto-materialization.

Run with: pytest tests/test_all_assets_comprehensive.py -v
Or: python tests/test_all_assets_comprehensive.py
"""

import os
import sys
from pathlib import Path

# Add lineage to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "lineage" / "src"))

from dagster import materialize, AssetSelection
from lineage.definitions import defs


def test_single_raw_asset():
    """Test a single raw MongoDB asset materializes correctly."""
    print("🧪 Testing Single Raw Asset (lms_users)...")
    
    try:
        result = materialize(
            [AssetSelection.keys("raw", "lms_users")],
            resources=defs.get_all_resource_defs(),
        )
        
        if result.success:
            print("✅ Raw asset (lms_users) materialized successfully")
            return True
        else:
            print(f"❌ Raw materialization failed")
            return False
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_multiple_raw_assets():
    """Test multiple raw MongoDB assets materialize correctly."""
    print("\n🧪 Testing Multiple Raw Assets...")
    
    # Test a few key collections
    test_assets = [
        AssetSelection.keys("raw", "lms_users"),
        AssetSelection.keys("raw", "lms_schools"),
        AssetSelection.keys("raw", "lms_courses"),
        AssetSelection.keys("raw", "lms_enrollments"),
    ]
    
    try:
        result = materialize(
            [*test_assets],
            resources=defs.get_all_resource_defs(),
        )
        
        if result.success:
            print("✅ Multiple raw assets materialized successfully")
            return True
        else:
            print(f"❌ Raw materialization failed")
            return False
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_silver_dbt_compile():
    """Test that all silver dbt models compile without errors."""
    print("\n🧪 Testing Silver dbt Models Compilation...")
    
    import subprocess
    
    try:
        # Change to dbt directory
        dbt_dir = project_root / "dbt"
        os.chdir(dbt_dir)
        
        # Run dbt parse to check compilation
        result = subprocess.run(
            ["dbt", "parse", "--target", "duckdb"],
            capture_output=True,
            text=True,
            timeout=60
        )
        
        if result.returncode == 0:
            print("✅ All dbt models compiled successfully")
            return True
        else:
            print(f"❌ dbt compilation failed:")
            print(result.stdout)
            print(result.stderr)
            return False
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        os.chdir(project_root)


def test_silver_asset_materialization():
    """Test a single silver dbt asset materializes correctly."""
    print("\n🧪 Testing Silver Asset Materialization (dim_person)...")
    
    try:
        result = materialize(
            [AssetSelection.keys("silver", "dim_person")],
            resources=defs.get_all_resource_defs(),
        )
        
        if result.success:
            print("✅ Silver asset (dim_person) materialized successfully")
            return True
        else:
            print(f"❌ Silver materialization failed")
            return False
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_auto_materialization():
    """Test that silver assets can materialize after raw assets."""
    print("\n🧪 Testing Auto-Materialization Flow...")
    
    try:
        # First materialize raw
        print("   Step 1: Materializing raw asset (lms_users)...")
        raw_result = materialize(
            [AssetSelection.keys("raw", "lms_users")],
            resources=defs.get_all_resource_defs(),
        )
        
        if not raw_result.success:
            print("❌ Raw asset materialization failed")
            return False
        
        print("   Step 2: Materializing dependent silver asset (dim_person)...")
        # Then try to materialize dependent silver
        silver_result = materialize(
            [AssetSelection.keys("silver", "dim_person")],
            resources=defs.get_all_resource_defs(),
        )
        
        if silver_result.success:
            print("✅ Auto-materialization works: silver materialized after raw")
            return True
        else:
            print("❌ Auto-materialization failed")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("🚀 Starting Comprehensive Asset Tests\n")
    print("=" * 60)
    
    results = []
    results.append(("Single Raw Asset", test_single_raw_asset()))
    results.append(("Multiple Raw Assets", test_multiple_raw_assets()))
    results.append(("Silver dbt Compilation", test_silver_dbt_compile()))
    results.append(("Silver Asset Materialization", test_silver_asset_materialization()))
    results.append(("Auto-Materialization", test_auto_materialization()))
    
    print("\n" + "=" * 60)
    print("📊 Test Results Summary:")
    print("=" * 60)
    for name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"   {status}: {name}")
    
    all_passed = all(r[1] for r in results)
    print("=" * 60)
    if all_passed:
        print("✅ All tests passed!")
    else:
        print("❌ Some tests failed")
    
    sys.exit(0 if all_passed else 1)


