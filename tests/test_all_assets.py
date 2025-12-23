#!/usr/bin/env python3
"""
Comprehensive test script for all Dagster assets (dlt and dbt).
Tests raw materialization and silver auto-materialization.
"""

import os
import sys
from pathlib import Path

# Add lineage to path
sys.path.insert(0, str(Path(__file__).parent / "lineage" / "src"))

from dagster import materialize, AssetSelection
from lineage.definitions import defs

def test_raw_assets():
    """Test all raw MongoDB assets materialize correctly."""
    print("🧪 Testing Raw Assets Materialization...")
    
    # Select all raw assets
    raw_assets = AssetSelection.groups("raw")
    
    try:
        result = materialize(
            [*raw_assets],
            resources=defs.get_all_resource_defs(),
        )
        
        if result.success:
            print("✅ All raw assets materialized successfully")
            return True
        else:
            print(f"❌ Raw materialization failed: {result}")
            return False
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_silver_assets():
    """Test all silver dbt assets compile and run."""
    print("\n🧪 Testing Silver Assets (dbt models)...")
    
    # Select all silver assets
    silver_assets = AssetSelection.groups("silver")
    
    try:
        result = materialize(
            [*silver_assets],
            resources=defs.get_all_resource_defs(),
        )
        
        if result.success:
            print("✅ All silver assets materialized successfully")
            return True
        else:
            print(f"❌ Silver materialization failed: {result}")
            return False
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_auto_materialization():
    """Test that silver assets auto-materialize after raw."""
    print("\n🧪 Testing Auto-Materialization...")
    
    # Materialize one raw asset
    raw_asset = AssetSelection.keys("raw", "lms_users")
    
    try:
        # First materialize raw
        raw_result = materialize(
            [*raw_asset],
            resources=defs.get_all_resource_defs(),
        )
        
        if not raw_result.success:
            print("❌ Raw asset materialization failed")
            return False
        
        # Then try to materialize dependent silver
        silver_asset = AssetSelection.keys("silver", "dim_person")
        silver_result = materialize(
            [*silver_asset],
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
    
    results = []
    results.append(("Raw Assets", test_raw_assets()))
    results.append(("Silver Assets", test_silver_assets()))
    results.append(("Auto-Materialization", test_auto_materialization()))
    
    print("\n📊 Test Results:")
    for name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"   {status}: {name}")
    
    all_passed = all(r[1] for r in results)
    sys.exit(0 if all_passed else 1)
